//
// Created by Shujian Qian on 2023-10-11.
//

#include "benchmarks/tpcc_gpu_submitter.h"
#include "execution_planner.h"
#include "tpcc_gpu_txn.cuh"

#include "util_log.h"
#include "util_gpu_error_check.cuh"

#include <thrust/for_each.h>
#include <thrust/iterator/iterator_adaptor.h>
#include <cub/cub.cuh>
#include <cub/device/device_scan.cuh>
#include <thrust/system/cuda/detail/reduce.h>

namespace epic::tpcc {

template <typename TxnParamArrayType>
TpccGpuSubmitter<TxnParamArrayType>::TpccGpuSubmitter(TableSubmitDest warehouse_submit_dest, TableSubmitDest district_submit_dest,
    TableSubmitDest customer_submit_dest, TableSubmitDest history_submit_dest, TableSubmitDest new_order_submit_dest,
    TableSubmitDest order_submit_dest, TableSubmitDest order_line_submit_dest, TableSubmitDest item_submit_dest,
    TableSubmitDest stock_submit_dest)
    : TpccSubmitter<TxnParamArrayType>(warehouse_submit_dest, district_submit_dest, customer_submit_dest, history_submit_dest,
          new_order_submit_dest, order_submit_dest, order_line_submit_dest, item_submit_dest, stock_submit_dest)
{
    for (int i = 0; i < 9; i++)
    {
        cudaStream_t stream;
        gpu_err_check(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking));
        // gpu_err_check(cudaStreamCreate(&stream));
        cuda_streams.emplace_back(stream);
    }
}

template <typename TxnParamArrayType>
TpccGpuSubmitter<TxnParamArrayType>::~TpccGpuSubmitter()
{
    for (auto &stream : cuda_streams)
    {
        if (stream.has_value())
        {
            gpu_err_check(cudaStreamDestroy(std::any_cast<cudaStream_t>(stream)));
        }
    }
}

struct TpccNumOps
{
    uint32_t *warehouse_num_ops;
    uint32_t *district_num_ops;
    uint32_t *customer_num_ops;
    uint32_t *history_num_ops;
    uint32_t *order_num_ops;
    uint32_t *new_order_num_ops;
    uint32_t *order_line_num_ops;
    uint32_t *item_num_ops;
    uint32_t *stock_num_ops;
};

struct TpccSubmitLocations
{
    uint32_t *warehouse_offset;
    uint32_t *district_offset;
    uint32_t *customer_offset;
    uint32_t *history_offset;
    uint32_t *order_offset;
    uint32_t *new_order_offset;
    uint32_t *order_line_offset;
    uint32_t *item_offset;
    uint32_t *stock_offset;
    void *warehouse_dest;
    void *district_dest;
    void *customer_dest;
    void *history_dest;
    void *order_dest;
    void *new_order_dest;
    void *order_line_dest;
    void *item_dest;
    void *stock_dest;
};

static __device__ __forceinline__ void prepareSubmitTpccTxn(
    int txn_id, NewOrderTxnParams<FixedSizeTxn> *txn, TpccNumOps num_ops)
{
    num_ops.warehouse_num_ops[txn_id] = 1;
    num_ops.district_num_ops[txn_id] = 2;
    num_ops.customer_num_ops[txn_id] = 1;
    num_ops.history_num_ops[txn_id] = 0;
    num_ops.order_num_ops[txn_id] = 1;
    num_ops.new_order_num_ops[txn_id] = 1;
    uint32_t num_items = txn->num_items;
    num_ops.order_line_num_ops[txn_id] = num_items;
    num_ops.item_num_ops[txn_id] = num_items;
    num_ops.stock_num_ops[txn_id] = num_items * 2;
}

static __device__ __forceinline__ void prepareSubmitTpccTxn(int txn_id, PaymentTxnParams *txn, TpccNumOps num_ops)
{
    num_ops.warehouse_num_ops[txn_id] = 2;
    num_ops.district_num_ops[txn_id] = 2;
    num_ops.customer_num_ops[txn_id] = 2;
    num_ops.history_num_ops[txn_id] = 0; /* TODO: deal with history table later */
    num_ops.order_num_ops[txn_id] = 0;
    num_ops.new_order_num_ops[txn_id] = 0;
    num_ops.order_line_num_ops[txn_id] = 0;
    num_ops.item_num_ops[txn_id] = 0;
    num_ops.stock_num_ops[txn_id] = 0;
}

static __device__ __forceinline__ void prepareSubmitTpccTxn(int txn_id, OrderStatusTxnParams *txn, TpccNumOps num_ops)
{
    num_ops.warehouse_num_ops[txn_id] = 0;
    num_ops.district_num_ops[txn_id] = 0;
    num_ops.customer_num_ops[txn_id] = 1;
    num_ops.history_num_ops[txn_id] = 0;
    num_ops.order_num_ops[txn_id] = 1;
    num_ops.new_order_num_ops[txn_id] = 0;
    num_ops.order_line_num_ops[txn_id] = txn->num_items;
    num_ops.item_num_ops[txn_id] = 0;
    num_ops.stock_num_ops[txn_id] = 0;
}

static void __device__ __forceinline__ prepareSubmitTpccTxn(int txn_id, DeliveryTxnParams *txn, TpccNumOps num_ops)
{
    int orderline_ops = 0;
    for (int i = 0; i < 10; ++i)
    {
        orderline_ops += txn->num_items[i];
    }
    // printf("txn[%d] submit total_items[%d] ptr[%p] "
    //     "item0[%d] "
    //     "item1[%d] "
    //     "item2[%d] "
    //     "item3[%d] "
    //     "item4[%d] "
    //     "item5[%d] "
    //     "item6[%d] "
    //     "item7[%d] "
    //     "item8[%d] "
    //     "item9[%d] "
    //     "\n", txn_id, orderline_ops, txn,
    //     txn->num_items[0],
    //     txn->num_items[1],
    //     txn->num_items[2],
    //     txn->num_items[3],
    //     txn->num_items[4],
    //     txn->num_items[5],
    //     txn->num_items[6],
    //     txn->num_items[7],
    //     txn->num_items[8],
    //     txn->num_items[9]
    //     );
    //
    num_ops.warehouse_num_ops[txn_id] = 0;
    num_ops.district_num_ops[txn_id] = 0;
    num_ops.customer_num_ops[txn_id] = 20;
    num_ops.history_num_ops[txn_id] = 0;
    num_ops.order_num_ops[txn_id] = 20;
    num_ops.new_order_num_ops[txn_id] = 10;
    num_ops.order_line_num_ops[txn_id] = orderline_ops * 2;
    num_ops.item_num_ops[txn_id] = 0;
    num_ops.stock_num_ops[txn_id] = 0;

    // num_ops.new_order_num_ops[txn_id] = 0;
    // num_ops.customer_num_ops[txn_id] = 0;
    // num_ops.order_num_ops[txn_id] = 0; // TODO: remove
    // num_ops.order_line_num_ops[txn_id] = 0; // TODO: remove
    // num_ops.order_line_num_ops[txn_id] = 0; // TODO: remove
}

static void __device__ __forceinline__ prepareSubmitTpccTxn(int txn_id, StockLevelTxnParams *txn, TpccNumOps num_ops)
{
    num_ops.warehouse_num_ops[txn_id] = 0;
    num_ops.district_num_ops[txn_id] = 0;
    num_ops.customer_num_ops[txn_id] = 0;
    num_ops.history_num_ops[txn_id] = 0;
    num_ops.order_num_ops[txn_id] = 0;
    num_ops.new_order_num_ops[txn_id] = 0;
    num_ops.order_line_num_ops[txn_id] = 0;
    num_ops.item_num_ops[txn_id] = 0;
    num_ops.stock_num_ops[txn_id] = txn->num_items;
}

template <typename GpuTxnArrayType>
static __global__ void prepareSubmitTpccTxn(GpuTxnArrayType txn_array, TpccNumOps num_ops)
{
    int txn_id = blockIdx.x * blockDim.x + threadIdx.x;
    if (txn_id >= txn_array.num_txns)
    {
        return;
    }
    BaseTxn *base_txn = txn_array.getTxn(txn_id);
    switch (static_cast<TpccTxnType>(base_txn->txn_type))
    {
    case TpccTxnType::NEW_ORDER:
        prepareSubmitTpccTxn(txn_id, reinterpret_cast<NewOrderTxnParams<FixedSizeTxn> *>(base_txn->data), num_ops);
        break;
    case TpccTxnType::PAYMENT:
        prepareSubmitTpccTxn(txn_id, reinterpret_cast<PaymentTxnParams *>(base_txn->data), num_ops);
        break;
    case TpccTxnType::ORDER_STATUS:
        prepareSubmitTpccTxn(txn_id, reinterpret_cast<OrderStatusTxnParams *>(base_txn->data), num_ops);
        break;
    case TpccTxnType::DELIVERY:
        prepareSubmitTpccTxn(txn_id, reinterpret_cast<DeliveryTxnParams *>(base_txn->data), num_ops);
        break;
    case TpccTxnType::STOCK_LEVEL:
        prepareSubmitTpccTxn(txn_id, reinterpret_cast<StockLevelTxnParams *>(base_txn->data), num_ops);
        break;
    default:
        assert(false);
    }
}

static __device__ __forceinline__ void submitTpccTxn(
    int txn_id, NewOrderTxnParams<FixedSizeTxn> *txn, TpccSubmitLocations submit_loc)
{
    static_cast<uint64_t *>(submit_loc.warehouse_dest)[submit_loc.warehouse_offset[txn_id]] = CREATE_OP(
        txn->warehouse_id, txn_id, read_op, offsetof(NewOrderExecPlan<FixedSizeTxn>, warehouse_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.district_dest)[submit_loc.district_offset[txn_id]] = CREATE_OP(
        txn->district_id, txn_id, read_op, offsetof(NewOrderExecPlan<FixedSizeTxn>, district_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.district_dest)[submit_loc.district_offset[txn_id] + 1] = CREATE_OP(
        txn->district_id, txn_id, write_op, offsetof(NewOrderExecPlan<FixedSizeTxn>, district_write_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.customer_dest)[submit_loc.customer_offset[txn_id]] = CREATE_OP(
        txn->customer_id, txn_id, read_op, offsetof(NewOrderExecPlan<FixedSizeTxn>, customer_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.order_dest)[submit_loc.order_offset[txn_id]] = CREATE_OP(
        txn->order_id, txn_id, write_op, offsetof(NewOrderExecPlan<FixedSizeTxn>, order_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.new_order_dest)[submit_loc.new_order_offset[txn_id]] =
        CREATE_OP(txn->new_order_id, txn_id, write_op,
            offsetof(NewOrderExecPlan<FixedSizeTxn>, new_order_loc) / sizeof(uint32_t));
    for (int i = 0; i < txn->num_items; i++)
    {
        static_cast<uint64_t *>(submit_loc.order_line_dest)[submit_loc.order_line_offset[txn_id] + i] =
            CREATE_OP(txn->items[i].order_line_id, txn_id, write_op,
                offsetof(NewOrderExecPlan<FixedSizeTxn>, item_plans[i].orderline_loc) / sizeof(uint32_t));
        static_cast<uint64_t *>(submit_loc.item_dest)[submit_loc.item_offset[txn_id] + i] =
            CREATE_OP(txn->items[i].item_id, txn_id, read_op,
                offsetof(NewOrderExecPlan<FixedSizeTxn>, item_plans[i].item_loc) / sizeof(uint32_t));
        static_cast<uint64_t *>(submit_loc.stock_dest)[submit_loc.stock_offset[txn_id] + i * 2] =
            CREATE_OP(txn->items[i].stock_id, txn_id, read_op,
                offsetof(NewOrderExecPlan<FixedSizeTxn>, item_plans[i].stock_read_loc) / sizeof(uint32_t));
        static_cast<uint64_t *>(submit_loc.stock_dest)[submit_loc.stock_offset[txn_id] + i * 2 + 1] =
            CREATE_OP(txn->items[i].stock_id, txn_id, write_op,
                offsetof(NewOrderExecPlan<FixedSizeTxn>, item_plans[i].stock_write_loc) / sizeof(uint32_t));
    }
    __threadfence(); /* TODO: remove this */
}

static __device__ __forceinline__ void submitTpccTxn(int txn_id, PaymentTxnParams *txn, TpccSubmitLocations submit_loc)
{
    static_cast<uint64_t *>(submit_loc.warehouse_dest)[submit_loc.warehouse_offset[txn_id]] = CREATE_OP(
        txn->warehouse_id, txn_id, read_op, offsetof(PaymentTxnExecPlan, warehouse_read_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.warehouse_dest)[submit_loc.warehouse_offset[txn_id] + 1] = CREATE_OP(
        txn->warehouse_id, txn_id, write_op, offsetof(PaymentTxnExecPlan, warehouse_write_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.district_dest)[submit_loc.district_offset[txn_id]] = CREATE_OP(
        txn->district_id, txn_id, read_op, offsetof(PaymentTxnExecPlan, district_read_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.district_dest)[submit_loc.district_offset[txn_id] + 1] = CREATE_OP(
        txn->district_id, txn_id, write_op, offsetof(PaymentTxnExecPlan, district_write_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.customer_dest)[submit_loc.customer_offset[txn_id]] = CREATE_OP(
        txn->customer_id, txn_id, read_op, offsetof(PaymentTxnExecPlan, customer_read_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.customer_dest)[submit_loc.customer_offset[txn_id] + 1] = CREATE_OP(
        txn->customer_id, txn_id, write_op, offsetof(PaymentTxnExecPlan, customer_write_loc) / sizeof(uint32_t));
    __threadfence(); /* TODO: remove this */
}

static __device__ __forceinline__ void submitTpccTxn(int txn_id, OrderStatusTxnParams *txn, TpccSubmitLocations submit_loc)
{
    static_cast<uint64_t *>(submit_loc.customer_dest)[submit_loc.customer_offset[txn_id]] = CREATE_OP(
        txn->customer_id, txn_id, read_op, offsetof(OrderStatusTxnExecPlan, customer_loc) / sizeof(uint32_t));
    static_cast<uint64_t *>(submit_loc.order_dest)[submit_loc.order_offset[txn_id]] = CREATE_OP(
        txn->order_id, txn_id, read_op, offsetof(OrderStatusTxnExecPlan, order_loc) / sizeof(uint32_t));
    for (int i = 0; i < txn->num_items; i++)
    {
        static_cast<uint64_t *>(submit_loc.order_line_dest)[submit_loc.order_line_offset[txn_id] + i] =
            CREATE_OP(txn->orderline_ids[i], txn_id, read_op,
                offsetof(OrderStatusTxnExecPlan, orderline_locs[i]) / sizeof(uint32_t));
    }
}

static __device__ __forceinline__ void submitTpccTxn(int txn_id, DeliveryTxnParams *txn, TpccSubmitLocations submit_loc)
{
    int orderline_ops = 0;
    for (int i = 0; i < 10; ++i)
    {
        int loc_offset = i * 2;
        static_cast<uint64_t *>(submit_loc.customer_dest)[submit_loc.customer_offset[txn_id] + loc_offset] = CREATE_OP(
            txn->customer_id[i], txn_id, read_op, offsetof(DeliveryTxnExecPlan, customer_read_locs[i]) / sizeof(uint32_t));
        static_cast<uint64_t *>(submit_loc.customer_dest)[submit_loc.customer_offset[txn_id] + loc_offset + 1] = CREATE_OP(
            txn->customer_id[i], txn_id, write_op, offsetof(DeliveryTxnExecPlan, customer_write_locs[i]) / sizeof(uint32_t));

        static_cast<uint64_t *>(submit_loc.new_order_dest)[submit_loc.new_order_offset[txn_id] + i] = CREATE_OP(
            txn->new_order_id[i], txn_id, read_op, offsetof(DeliveryTxnExecPlan, new_order_read_locs[i]) / sizeof(uint32_t));

        static_cast<uint64_t *>(submit_loc.order_dest)[submit_loc.order_offset[txn_id] + loc_offset] = CREATE_OP(
            txn->order_id[i], txn_id, read_op, offsetof(DeliveryTxnExecPlan, order_read_locs[i]) / sizeof(uint32_t));
        static_cast<uint64_t *>(submit_loc.order_dest)[submit_loc.order_offset[txn_id] + loc_offset + 1] = CREATE_OP(
            txn->order_id[i], txn_id, write_op, offsetof(DeliveryTxnExecPlan, order_write_locs[i]) / sizeof(uint32_t));


        for (int j = 0; j < txn->num_items[i]; ++j)
        {
            static_cast<uint64_t *>(submit_loc.order_line_dest)[submit_loc.order_line_offset[txn_id] + orderline_ops] =
                CREATE_OP(txn->orderline_ids[i][j], txn_id, read_op,
                    offsetof(DeliveryTxnExecPlan, orderline_read_locs[i][j]) / sizeof(uint32_t));
            orderline_ops++;
            static_cast<uint64_t *>(submit_loc.order_line_dest)[submit_loc.order_line_offset[txn_id] + orderline_ops] =
                CREATE_OP(txn->orderline_ids[i][j], txn_id, write_op,
                    offsetof(DeliveryTxnExecPlan, orderline_write_locs[i][j]) / sizeof(uint32_t));
            orderline_ops++;
        }
    }
}

static __device__ __forceinline__ void submitTpccTxn(
    int txn_id, StockLevelTxnParams *txn, TpccSubmitLocations submit_loc)
{
    uint32_t stock_offset = submit_loc.stock_offset[txn_id];
    for (uint32_t i = 0; i < txn->num_items; ++i)
    {
        static_cast<uint64_t *>(submit_loc.stock_dest)[stock_offset + i] = CREATE_OP(
            txn->stock_ids[i], txn_id, read_op, offsetof(StockLevelTxnExecPlan, stock_read_locs[i]) / sizeof(uint32_t));
    }
}

template <typename GpuTxnArrayType>
static __global__ void submitTpccTxn(GpuTxnArrayType txn_array, TpccSubmitLocations submit_loc)
{
    int txn_id = blockIdx.x * blockDim.x + threadIdx.x;
    if (txn_id >= txn_array.num_txns)
    {
        return;
    }
    BaseTxn *base_txn = txn_array.getTxn(txn_id);
    switch (static_cast<TpccTxnType>(base_txn->txn_type))
    {
    case TpccTxnType::NEW_ORDER:
        submitTpccTxn(txn_id, reinterpret_cast<NewOrderTxnParams<FixedSizeTxn> *>(base_txn->data), submit_loc);
        break;
    case TpccTxnType::PAYMENT:
        submitTpccTxn(txn_id, reinterpret_cast<PaymentTxnParams *>(base_txn->data), submit_loc);
        break;
    case TpccTxnType::ORDER_STATUS:
        submitTpccTxn(txn_id, reinterpret_cast<OrderStatusTxnParams *>(base_txn->data), submit_loc);
        break;
    case TpccTxnType::DELIVERY:
        submitTpccTxn(txn_id, reinterpret_cast<DeliveryTxnParams *>(base_txn->data), submit_loc);
        break;
    case TpccTxnType::STOCK_LEVEL:
        submitTpccTxn(txn_id, reinterpret_cast<StockLevelTxnParams *>(base_txn->data), submit_loc);
        break;
    default:
        assert(false);
    }
}

template <typename TxnParamArrayType>
void TpccGpuSubmitter<TxnParamArrayType>::submit(TxnParamArrayType &txn_array)
{
    auto &logger = Logger::GetInstance();

    TpccNumOps num_ops = {.warehouse_num_ops = warehouse_submit_dest.d_num_ops,
        .district_num_ops = district_submit_dest.d_num_ops,
        .customer_num_ops = customer_submit_dest.d_num_ops,
        .history_num_ops = history_submit_dest.d_num_ops,
        .order_num_ops = order_submit_dest.d_num_ops,
        .new_order_num_ops = new_order_submit_dest.d_num_ops,
        .order_line_num_ops = order_line_submit_dest.d_num_ops,
        .item_num_ops = item_submit_dest.d_num_ops,
        .stock_num_ops = stock_submit_dest.d_num_ops};

    prepareSubmitTpccTxn<<<(txn_array.num_txns + 1024) / 1024, 1024, 0, std::any_cast<cudaStream_t>(cuda_streams[0])>>>(
        TpccGpuTxnArrayT(txn_array), num_ops);

    gpu_err_check(cudaGetLastError());
    gpu_err_check(cudaStreamSynchronize(std::any_cast<cudaStream_t>(cuda_streams[0])));

    gpu_err_check(cub::DeviceScan::InclusiveSum(warehouse_submit_dest.temp_storage,
        warehouse_submit_dest.temp_storage_bytes, warehouse_submit_dest.d_num_ops,
        warehouse_submit_dest.d_op_offsets + 1, txn_array.num_txns, std::any_cast<cudaStream_t>(cuda_streams[0])));
    gpu_err_check(cub::DeviceScan::InclusiveSum(district_submit_dest.temp_storage,
        district_submit_dest.temp_storage_bytes, district_submit_dest.d_num_ops, district_submit_dest.d_op_offsets + 1,
        txn_array.num_txns, std::any_cast<cudaStream_t>(cuda_streams[1])));
    gpu_err_check(cub::DeviceScan::InclusiveSum(customer_submit_dest.temp_storage,
        customer_submit_dest.temp_storage_bytes, customer_submit_dest.d_num_ops, customer_submit_dest.d_op_offsets + 1,
        txn_array.num_txns, std::any_cast<cudaStream_t>(cuda_streams[2])));
    gpu_err_check(cub::DeviceScan::InclusiveSum(history_submit_dest.temp_storage,
        history_submit_dest.temp_storage_bytes, history_submit_dest.d_num_ops, history_submit_dest.d_op_offsets + 1,
        txn_array.num_txns, std::any_cast<cudaStream_t>(cuda_streams[3])));
    gpu_err_check(cub::DeviceScan::InclusiveSum(order_submit_dest.temp_storage, order_submit_dest.temp_storage_bytes,
        order_submit_dest.d_num_ops, order_submit_dest.d_op_offsets + 1, txn_array.num_txns,
        std::any_cast<cudaStream_t>(cuda_streams[4])));
    gpu_err_check(cub::DeviceScan::InclusiveSum(new_order_submit_dest.temp_storage,
        new_order_submit_dest.temp_storage_bytes, new_order_submit_dest.d_num_ops,
        new_order_submit_dest.d_op_offsets + 1, txn_array.num_txns, std::any_cast<cudaStream_t>(cuda_streams[5])));
    gpu_err_check(cub::DeviceScan::InclusiveSum(order_line_submit_dest.temp_storage,
        order_line_submit_dest.temp_storage_bytes, order_line_submit_dest.d_num_ops,
        order_line_submit_dest.d_op_offsets + 1, txn_array.num_txns, std::any_cast<cudaStream_t>(cuda_streams[6])));
    gpu_err_check(cub::DeviceScan::InclusiveSum(item_submit_dest.temp_storage, item_submit_dest.temp_storage_bytes,
        item_submit_dest.d_num_ops, item_submit_dest.d_op_offsets + 1, txn_array.num_txns,
        std::any_cast<cudaStream_t>(cuda_streams[7])));
    gpu_err_check(cub::DeviceScan::InclusiveSum(stock_submit_dest.temp_storage, stock_submit_dest.temp_storage_bytes,
        stock_submit_dest.d_num_ops, stock_submit_dest.d_op_offsets + 1, txn_array.num_txns,
        std::any_cast<cudaStream_t>(cuda_streams[8])));

    gpu_err_check(
        cudaMemcpyAsync(&warehouse_submit_dest.curr_num_ops, warehouse_submit_dest.d_op_offsets + txn_array.num_txns,
            sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[0])));
    gpu_err_check(
        cudaMemcpyAsync(&district_submit_dest.curr_num_ops, district_submit_dest.d_op_offsets + txn_array.num_txns,
            sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[1])));
    gpu_err_check(
        cudaMemcpyAsync(&customer_submit_dest.curr_num_ops, customer_submit_dest.d_op_offsets + txn_array.num_txns,
            sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[2])));
    gpu_err_check(
        cudaMemcpyAsync(&history_submit_dest.curr_num_ops, history_submit_dest.d_op_offsets + txn_array.num_txns,
            sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[3])));
    gpu_err_check(cudaMemcpyAsync(&order_submit_dest.curr_num_ops, order_submit_dest.d_op_offsets + txn_array.num_txns,
        sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[4])));
    gpu_err_check(
        cudaMemcpyAsync(&new_order_submit_dest.curr_num_ops, new_order_submit_dest.d_op_offsets + txn_array.num_txns,
            sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[5])));
    gpu_err_check(
        cudaMemcpyAsync(&order_line_submit_dest.curr_num_ops, order_line_submit_dest.d_op_offsets + txn_array.num_txns,
            sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[6])));
    gpu_err_check(cudaMemcpyAsync(&item_submit_dest.curr_num_ops, item_submit_dest.d_op_offsets + txn_array.num_txns,
        sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[7])));
    gpu_err_check(cudaMemcpyAsync(&stock_submit_dest.curr_num_ops, stock_submit_dest.d_op_offsets + txn_array.num_txns,
        sizeof(uint32_t), cudaMemcpyDeviceToHost, std::any_cast<cudaStream_t>(cuda_streams[8])));

    TpccSubmitLocations locs = {
        .warehouse_offset = warehouse_submit_dest.d_op_offsets,
        .district_offset = district_submit_dest.d_op_offsets,
        .customer_offset = customer_submit_dest.d_op_offsets,
        .history_offset = history_submit_dest.d_op_offsets,
        .order_offset = order_submit_dest.d_op_offsets,
        .new_order_offset = new_order_submit_dest.d_op_offsets,
        .order_line_offset = order_line_submit_dest.d_op_offsets,
        .item_offset = item_submit_dest.d_op_offsets,
        .stock_offset = stock_submit_dest.d_op_offsets,
        .warehouse_dest = warehouse_submit_dest.d_submitted_ops,
        .district_dest = district_submit_dest.d_submitted_ops,
        .customer_dest = customer_submit_dest.d_submitted_ops,
        .history_dest = history_submit_dest.d_submitted_ops,
        .order_dest = order_submit_dest.d_submitted_ops,
        .new_order_dest = new_order_submit_dest.d_submitted_ops,
        .order_line_dest = order_line_submit_dest.d_submitted_ops,
        .item_dest = item_submit_dest.d_submitted_ops,
        .stock_dest = stock_submit_dest.d_submitted_ops,
    };

    submitTpccTxn<<<(txn_array.num_txns + 1024) / 1024, 1024, 0, std::any_cast<cudaStream_t>(cuda_streams[0])>>>(
        TpccGpuTxnArrayT(txn_array), locs);

    gpu_err_check(cudaGetLastError());
    for (auto &stream : cuda_streams)
    {
        gpu_err_check(cudaStreamSynchronize(std::any_cast<cudaStream_t>(stream)));
    }

    logger.Info("num txns: {}", txn_array.num_txns);
    logger.Info("warehouse num ops: {}", warehouse_submit_dest.curr_num_ops);
    logger.Info("district num ops: {}", district_submit_dest.curr_num_ops);
    logger.Info("customer num ops: {}", customer_submit_dest.curr_num_ops);
    logger.Info("history num ops: {}", history_submit_dest.curr_num_ops);
    logger.Info("order num ops: {}", order_submit_dest.curr_num_ops);
    logger.Info("new order num ops: {}", new_order_submit_dest.curr_num_ops);
    logger.Info("order line num ops: {}", order_line_submit_dest.curr_num_ops);
    logger.Info("item num ops: {}", item_submit_dest.curr_num_ops);
    logger.Info("stock num ops: {}", stock_submit_dest.curr_num_ops);

#if 0 /* for debugging only */
    op_t ops[100];
    gpu_err_check(cudaMemcpy(ops, warehouse_submit_dest.d_submitted_ops, sizeof(op_t) * 100, cudaMemcpyDeviceToHost));
    for (int i = 0; i < 100; i++)
    {
        logger.Info("op{}: record[{}] txn[{}] rw[{}] offset[{}]", i, GET_RECORD_ID(ops[i]), GET_TXN_ID(ops[i]),
            GET_R_W(ops[i]), GET_OFFSET(ops[i]));
    }
#endif
}

template class TpccGpuSubmitter<TpccTxnParamArrayT>;

} // namespace epic::tpcc