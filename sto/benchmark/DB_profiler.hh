#pragma once

#include <chrono>
#include <iomanip>

#include "SystemProfiler.hh"
#include "Transaction.hh"
#include "DB_params.hh"

namespace bench {

class db_profiler {
public:
    using constants = db_params::constants;
    explicit db_profiler(bool spawn_perf)
            : spawn_perf_(spawn_perf), perf_pid_(),
              start_tsc_(), end_tsc_() {}

    static double ticks_to_secs(const uint64_t ticks) {
        return (double)ticks / constants::billion / constants::processor_tsc_frequency;
    }

    static uint64_t secs_to_ticks(const double secs) {
        return (uint64_t)(secs * constants::billion * constants::processor_tsc_frequency);
    }

    void start(Profiler::perf_mode mode) {
        if (spawn_perf_)
            perf_pid_ = Profiler::spawn("perf", mode);
        start_time_ = std::chrono::high_resolution_clock::now();
        start_tsc_ = read_tsc();
    }

    uint64_t start_timestamp() const {
        return start_tsc_;
    }

    double finish(size_t num_txns) {
        end_tsc_ = read_tsc();
        end_time_ = std::chrono::high_resolution_clock::now();
        if (spawn_perf_) {
            bool ok = Profiler::stop(perf_pid_);
            always_assert(ok, "killing profiler");
        }
        // print elapsed time
        uint64_t elapsed_tsc = end_tsc_ - start_tsc_;
        double elapsed_time = (double) elapsed_tsc / constants::million / constants::processor_tsc_frequency;
        uint64_t wallclock_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time_ - start_time_).count();
        std::cout << "Elapsed time: " << elapsed_tsc << " ticks" << std::endl;
        std::cout << "Estimated time: " << elapsed_time << " ms" << std::endl;
        std::cout << "Wallclock time: " << wallclock_time << " us" << std::endl;
        std::cout << "Throughput: " << (double) num_txns / (elapsed_time / 1000.0) << " txns/sec" << std::endl;
        std::cout << "Real throughput: " << std::setprecision (15) << (double) num_txns / wallclock_time << " MTxns/sec" << std::endl;

        // print STO stats
        Transaction::print_stats();

        // return elapsed ms
        return elapsed_time;
    }

private:
    bool spawn_perf_;
    pid_t perf_pid_;
    uint64_t start_tsc_;
    uint64_t end_tsc_;
    std::chrono::high_resolution_clock::time_point start_time_, end_time_;
};

}; // namespace bench

