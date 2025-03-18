#pragma once


#include <set>
#include "YCSB_bench.hh"
#include "YCSB_structs.hh"

namespace ycsb {

static constexpr uint64_t max_txns = 200000;

template <typename DBParams>
void ycsb_runner<DBParams>::gen_workload(uint64_t threadid, int txn_size, double ycsb_skew, bool full_read) {
    dist_init(ycsb_skew);
    const int collapse = txn_size < 0 ? -txn_size : 0;
    int tsz_factor = 1;  // For collapse experiments
    bool write_first = false;  // For collapse experiments
    for (uint64_t i = 0; i < (collapse ? 20 : max_txns); ++i) {
        ycsb_txn_t txn {};
        std::set<uint32_t> key_set;
        uint8_t collapse_type = 0;
        if (collapse) {
            // Type 1 is read-write, type 2 is write-only
            if (collapse == 1) {
                collapse_type = threadid ? 1 : 2;
                txn_size = 16;
                tsz_factor = 1;
                write_first = false;
            } else if (collapse == 2) {
                collapse_type = threadid ? 1 : 2;
                txn_size = 16;
                tsz_factor = 1;
                write_first = true;
            } else if (collapse == 3) {
                collapse_type = threadid ? 2 : 1;
                txn_size = 16;
                tsz_factor = collapse_type == 2 ? 64 : 1;
                write_first = true;
            }
        }
        txn.ops.reserve(txn_size);
        if (collapse) {
            uint32_t key = dd->sample() % (txn_size * tsz_factor);
            for (int j = 0; j < txn_size; ++j) {
                key_set.insert(key);
                key = (key + 1) % (txn_size * tsz_factor);
            }
        } else {
            for (int j = 0; j < txn_size; ++j) {
                uint32_t key;
                do {
                    key = dd->sample();
                } while (key_set.find(key) != key_set.end());
                key_set.insert(key);
            }
        }
        bool any_write = false;
        for (auto it = key_set.begin(); it != key_set.end(); ++it) {
            ycsb_op_t op {};
            if (collapse) {
                op.is_write = (collapse_type == 2) || (write_first && it == key_set.begin());
                txn.collapse_type = collapse_type;
            } else {
                op.is_write = ud->sample() < write_threshold;
            }
            if (op.is_write) {
                if (mode == mode_id::YCSB_A || mode == mode_id::YCSB_B || mode == mode_id::YCSB_C) {
                    op.op_type = ycsb_op_t::YcsbOpType::WRITE;
                } else if (mode == mode_id::YCSB_F) {
                    op.op_type = ycsb_op_t::YcsbOpType::RMW;
                } 
            } else {
                if (full_read) {
                    op.op_type = ycsb_op_t::YcsbOpType::FULLREAD;
                } else {
                    op.op_type = ycsb_op_t::YcsbOpType::READ;
                }
            }
            op.key = *it;
            op.col_n = ud->sample() % (2*HALF_NUM_COLUMNS); /*column number*/
            if (op.is_write) {
                any_write = true;
                ig.random_ycsb_col_value_inplace(&op.write_value);
            }
            txn.ops.push_back(std::move(op));
        }
        txn.rw_txn = any_write;
        workload.push_back(std::move(txn));
    }
}

using bench::access_t;

template <typename DBParams>
void ycsb_runner<DBParams>::run_txn(const ycsb_txn_t& txn) {
    col_type output;
    typedef ycsb_value::NamedColumn nm;

    /* mod: this cannot prevent the copy from being optimized away. */
    (void)output;

    TRANSACTION {
        if (DBParams::MVCC && txn.rw_txn) {
            Sto::mvcc_rw_upgrade();
        }
        for (auto& op : txn.ops) {
            bool col_parity = op.col_n % 2;
            auto col_group = col_parity ? nm::odd_columns : nm::even_columns;
            (void)col_group;
            if (op.is_write) {
                ycsb_key key(op.key);

                /* mod: RMW cannot do update even in Commute mode */
                access_t access_type;
                if (Commute && op.op_type != ycsb_op_t::YcsbOpType::RMW) {
                    access_type = access_t::write;  // note: write here means blind write
                } else {
                    access_type = access_t::update;
                }

                auto [success, result, row, value]
                    = db.ycsb_table().select_split_row(key,
                    {{col_group, access_type}}
                );

                (void)result;
                TXN_DO(success);
                assert(result);

                if (Commute && op.op_type != ycsb_op_t::YcsbOpType::RMW) {
                    commutators::Commutator<ycsb_value> comm(op.col_n, op.write_value);
                    db.ycsb_table().update_row(row, comm);
#if TABLE_FINE_GRAINED
                } else if (DBParams::MVCC) {
                    // MVCC loop also does a tx_alloc, so we don't need to do
                    // one here
                    ycsb_value new_val_base;
                    ycsb_value* new_val = &new_val_base;
                    if (col_parity) {
                        new_val->odd_columns = value.odd_columns();
                        new_val->odd_columns[op.col_n/2] = op.write_value;
                    } else {
                        new_val->even_columns = value.even_columns();
                        new_val->even_columns[op.col_n/2] = op.write_value;
                    }
                    db.ycsb_table().update_row(row, new_val);
#endif
                } else {
                    auto new_val = Sto::tx_alloc<ycsb_value>();
                    if (col_parity) {
                        new_val->odd_columns = value.odd_columns();
                        new_val->odd_columns[op.col_n/2] = op.write_value;
                    } else {
                        new_val->even_columns = value.even_columns();
                        new_val->even_columns[op.col_n/2] = op.write_value;
                    }
                    db.ycsb_table().update_row(row, new_val);
                }
            } else {
                ycsb_key key(op.key);
                if (op.op_type == ycsb_op_t::YcsbOpType::FULLREAD) {
                    /* mod: for full record read, read both groups and from all columns */
                    {
                        auto [success, result, row, value]
                            = db.ycsb_table().select_split_row(key, {{nm::odd_columns, access_t::read}});
                        (void)result; (void)row;
                        TXN_DO(success);
                        assert(result);
                        for (int col_n = 0; col_n < HALF_NUM_COLUMNS; col_n++) {
                            force_copy(&output, &(value.odd_columns()[col_n]));
                        }
                    }
                    {
                        auto [success, result, row, value]
                            = db.ycsb_table().select_split_row(key, {{nm::even_columns, access_t::read}});
                        (void)result; (void)row;
                        TXN_DO(success);
                        assert(result);
                        for (int col_n = 0; col_n < HALF_NUM_COLUMNS; col_n++) {
                            force_copy(&output, &(value.even_columns()[col_n]));
                        }
                    }
                } else {
                    auto [success, result, row, value]
                        = db.ycsb_table().select_split_row(key, {{col_group, access_t::read}});
                    (void)result; (void)row;
                    TXN_DO(success);
                    assert(result);

                    if (col_parity) {
                        force_copy(&output, &(value.odd_columns()[op.col_n / 2]));
                    } else {
                        force_copy(&output, &(value.even_columns()[op.col_n / 2]));
                    }
                }
            }
        }
    } RETRY(true);
}

};
