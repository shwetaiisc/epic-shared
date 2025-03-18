//
// Created by Shujian Qian on 2023-10-31.
//

#ifndef GPU_STORAGE_CUH
#define GPU_STORAGE_CUH

#include <storage.h>

#ifdef EPIC_CUDA_AVAILABLE

namespace epic {

/**
 *
 * @tparam ValueType
 * @param record
 * @param version
 * @param record_id
 * @param read_loc
 * @param epoch
 * @param lane_id
 * @param result
 * @param offset    offset in 32-bit words
 * @param length    length in 32-bit words
 */
template<typename ValueType>
__forceinline__ __device__ void gpuReadFromTableCoop(Record<ValueType> *record, Version<ValueType> *version,
    uint32_t record_id, uint32_t read_loc, uint32_t epoch, uint32_t &result, uint32_t lane_id, uint32_t offset = 0,
    uint32_t length = (sizeof(ValueType) + sizeof(uint32_t) - 1) / sizeof(uint32_t))
{
    constexpr uint32_t leader_lane_id = 0;
    constexpr uint32_t leader_lane_mask = 1u << leader_lane_id;
    constexpr uint32_t all_lanes_mask = 0xFFFFFFFFu;

    ValueType *value_to_read;
    if (lane_id == leader_lane_id)
    {
        if (read_loc == loc_record_a)
        {
            /* record A read */
            uint64_t combined_versions =
                atomicAdd(reinterpret_cast<unsigned long long *>(&record[record_id].version1), 0ull);
            /* TODO: atomicOperations should not be required here because all properly aligned read/write in CUDA
               happens atomically */
            uint32_t version1 = combined_versions & 0xFFFFFFFF;
            uint32_t version2 = combined_versions >> 32;
            if (version1 == epoch)
            {
                /* version 1 is written in this epoch (record_b) */
                value_to_read = &record[record_id].value2;
            }
            else if (version2 == epoch)
            {
                /* version 2 is written in this epoch (record_b) */
                value_to_read = &record[record_id].value1;
            }
            else if (version1 < version2)
            {
                /* version 2 is the latest version before this epoch (record_a) */
                value_to_read = &record[record_id].value2;
            }
            else
            {
                /* version 1 is the latest version before this epoch (record_a) */
                value_to_read = &record[record_id].value1;
            }
        }
        else if (read_loc == loc_record_b)
        {
            /* record B read */
            uint64_t combined_versions =
                atomicAdd(reinterpret_cast<unsigned long long *>(&record[record_id].version1), 0ull);
            uint32_t version1 = combined_versions & 0xFFFFFFFF;
            uint32_t version2 = combined_versions >> 32;
            if (version1 == epoch)
            {
                /* version 1 is written in this epoch (record_b) */
                value_to_read = &record[record_id].value1;
            }
            else if (version2 == epoch)
            {
                /* version 2 is written in this epoch (record_b) */
                value_to_read = &record[record_id].value2;
            }
            else if (version1 < version2)
            {
                /* version 1 will be written in this epoch (record_b) */
                value_to_read = &record[record_id].value1;
                /* wait until version 1 is written */
                while (atomicAdd(&record[record_id].version1, 0) != epoch) {}
            }
            else
            {
                /* version 2 will be written in this epohc (record_b) */
                value_to_read = &record[record_id].value2;
                /* wait until version 2 is written */
                while (atomicAdd(&record[record_id].version2, 0) != epoch) {}
            }
        }
        else
        {
            /* version read */
            value_to_read = &version[read_loc].value;
            /* wait until version is written */
            while (atomicAdd(&version[read_loc].version, 0) != epoch) {}
        }
    }
    /* broadcast the value to read */
    value_to_read = reinterpret_cast<ValueType *>(
        __shfl_sync(all_lanes_mask, reinterpret_cast<uintptr_t>(value_to_read), leader_lane_id));

    /* read into result */
    result = 0;
    for (int base = 0; base < length; base += 32)
    {
        if (lane_id + base < length)
        {
            result += reinterpret_cast<uint32_t *>(value_to_read)[offset + base + lane_id];
        }
    }
}

template<typename ValueType>
__forceinline__ __device__ void gpuReadFromTableThread(Record<ValueType> *record, Version<ValueType> *version,
    uint32_t record_id, uint32_t read_loc, uint32_t epoch, uint32_t &result, uint32_t offset = 0,
    uint32_t length = (sizeof(ValueType) + sizeof(uint32_t) - 1) / sizeof(uint32_t))
{
    constexpr uint32_t leader_lane_id = 0;
    constexpr uint32_t leader_lane_mask = 1u << leader_lane_id;
    constexpr uint32_t all_lanes_mask = 0xFFFFFFFFu;

    ValueType *value_to_read;
    if (read_loc == loc_record_a)
    {
        /* record A read */
        uint64_t combined_versions =
            atomicAdd(reinterpret_cast<unsigned long long *>(&record[record_id].version1), 0ull);
        /* TODO: atomicOperations should not be required here because all properly aligned read/write in CUDA
           happens atomically */
        uint32_t version1 = combined_versions & 0xFFFFFFFF;
        uint32_t version2 = combined_versions >> 32;
        if (version1 == epoch)
        {
            /* version 1 is written in this epoch (record_b) */
            value_to_read = &record[record_id].value2;
        }
        else if (version2 == epoch)
        {
            /* version 2 is written in this epoch (record_b) */
            value_to_read = &record[record_id].value1;
        }
        else if (version1 < version2)
        {
            /* version 2 is the latest version before this epoch (record_a) */
            value_to_read = &record[record_id].value2;
        }
        else
        {
            /* version 1 is the latest version before this epoch (record_a) */
            value_to_read = &record[record_id].value1;
        }
    }
    else if (read_loc == loc_record_b)
    {
        /* record B read */
        uint64_t combined_versions =
            atomicAdd(reinterpret_cast<unsigned long long *>(&record[record_id].version1), 0ull);
        uint32_t version1 = combined_versions & 0xFFFFFFFF;
        uint32_t version2 = combined_versions >> 32;
        if (version1 == epoch)
        {
            /* version 1 is written in this epoch (record_b) */
            value_to_read = &record[record_id].value1;
        }
        else if (version2 == epoch)
        {
            /* version 2 is written in this epoch (record_b) */
            value_to_read = &record[record_id].value2;
        }
        else if (version1 < version2)
        {
            /* version 1 will be written in this epoch (record_b) */
            value_to_read = &record[record_id].value1;
            /* wait until version 1 is written */
            while (atomicAdd(&record[record_id].version1, 0) != epoch) {}
        }
        else
        {
            /* version 2 will be written in this epohc (record_b) */
            value_to_read = &record[record_id].value2;
            /* wait until version 2 is written */
            while (atomicAdd(&record[record_id].version2, 0) != epoch) {}
        }
    }
    else
    {
        /* version read */
        value_to_read = &version[read_loc].value;
        /* wait until version is written */
        while (atomicAdd(&version[read_loc].version, 0) != epoch) {}
    }

    /* read into result */
    for (int i = 0; i < length; ++i)
    {
        result += reinterpret_cast<uint32_t *>(value_to_read)[offset + i];
    }
}

template<typename ValueType>
__forceinline__ __device__ void gpuReadMultipleFromTableCoop(Record<ValueType> *record, Version<ValueType> *version,
    uint32_t record_id, uint32_t read_loc, uint32_t epoch, uint32_t &result, uint32_t lane_id, uint32_t active_mask,
    uint32_t offset = 0, uint32_t length = (sizeof(ValueType) + sizeof(uint32_t) - 1) / sizeof(uint32_t))
{
    constexpr uint32_t leader_lane_id = 0;
    constexpr uint32_t leader_lane_mask = 1u << leader_lane_id;
    constexpr uint32_t all_lanes_mask = 0xFFFFFFFFu;

    uint32_t remaining_mask = active_mask;
    uint32_t lane_mask = 1 << lane_id;
    ValueType *value_to_read;
    uint32_t *epoch_to_read = nullptr;
    if (lane_mask & active_mask)
    {
        if (read_loc == loc_record_a)
        {
            /* record A read */
            uint64_t combined_versions =
                atomicAdd(reinterpret_cast<unsigned long long *>(&record[record_id].version1), 0ull);
            /* TODO: atomicOperations should not be required here because all properly aligned read/write in CUDA
               happens atomically */
            uint32_t version1 = combined_versions & 0xFFFFFFFF;
            uint32_t version2 = combined_versions >> 32;
            if (version1 == epoch)
            {
                /* version 1 is written in this epoch (record_b) */
                value_to_read = &record[record_id].value2;
            }
            else if (version2 == epoch)
            {
                /* version 2 is written in this epoch (record_b) */
                value_to_read = &record[record_id].value1;
            }
            else if (version1 < version2)
            {
                /* version 2 is the latest version before this epoch (record_a) */
                value_to_read = &record[record_id].value2;
            }
            else
            {
                /* version 1 is the latest version before this epoch (record_a) */
                value_to_read = &record[record_id].value1;
            }
        }
        else if (read_loc == loc_record_b)
        {
            /* record B read */
            uint64_t combined_versions =
                atomicAdd(reinterpret_cast<unsigned long long *>(&record[record_id].version1), 0ull);
            uint32_t version1 = combined_versions & 0xFFFFFFFF;
            uint32_t version2 = combined_versions >> 32;
            if (version1 == epoch)
            {
                /* version 1 is written in this epoch (record_b) */
                value_to_read = &record[record_id].value1;
            }
            else if (version2 == epoch)
            {
                /* version 2 is written in this epoch (record_b) */
                value_to_read = &record[record_id].value2;
            }
            else if (version1 < version2)
            {
                /* version 1 will be written in this epoch (record_b) */
                value_to_read = &record[record_id].value1;
                /* wait until version 1 is written */
                epoch_to_read = &record[record_id].version1;
            }
            else
            {
                /* version 2 will be written in this epohc (record_b) */
                value_to_read = &record[record_id].value2;
                /* wait until version 2 is written */
                epoch_to_read = &record[record_id].version2;
            }
        }
        else
        {
            /* version read */
            value_to_read = &version[read_loc].value;
            /* wait until version is written */
            epoch_to_read = &version[read_loc].version;
        }
    }

    while (remaining_mask)
    {
        uint32_t ready_mask;
        do
        {
            bool ready =
                !(lane_mask & remaining_mask) || epoch_to_read == nullptr || atomicAdd(epoch_to_read, 0) == epoch;
            ready_mask = __ballot_sync(remaining_mask, ready);
        } while (ready_mask == 0);
        remaining_mask &= ~ready_mask;
        while (ready_mask)
        {
            uint32_t ready_lane_id = __ffs(ready_mask) - 1;
            ValueType *curr_value_to_read = reinterpret_cast<ValueType *>(
                __shfl_sync(all_lanes_mask, reinterpret_cast<uintptr_t>(value_to_read), ready_lane_id));
            if (lane_id < length)
            {
                result = reinterpret_cast<uint32_t *>(curr_value_to_read)[offset + lane_id];
            }
            ready_mask &= ~(1u << ready_lane_id);
        }
    }
}

template<typename ValueType>
__forceinline__ __device__ void gpuWriteToTableCoop(Record<ValueType> *record, Version<ValueType> *version,
    uint32_t record_id, uint32_t write_loc, uint32_t epoch, uint32_t data, uint32_t lane_id, uint32_t offset = 0,
    uint32_t length = (sizeof(ValueType) + sizeof(uint32_t) - 1) / sizeof(uint32_t))
{
    constexpr uint32_t leader_lane_id = 0;
    constexpr uint32_t leader_lane_mask = 1u << leader_lane_id;
    constexpr uint32_t all_lanes_mask = 0xFFFFFFFFu;

    ValueType *value_to_write;
    uint32_t *version_to_update;
    if (lane_id == leader_lane_id)
    {
        if (write_loc == loc_record_b)
        {
            /* Record B write */
            uint64_t combined_versions =
                atomicAdd(reinterpret_cast<unsigned long long *>(&record[record_id].version1), 0ull);
            uint32_t version1 = combined_versions & 0xFFFFFFFF;
            uint32_t version2 = combined_versions >> 32;
            if (version1 < version2)
            {
                /* version 2 is the latest version before this epoch (record_a) */
                /* write to version 1 */
                value_to_write = &record[record_id].value1;
                version_to_update = &record[record_id].version1;
            }
            else
            {
                /* version 1 is the latest version before this epoch (record_a) */
                /* write to version 2 */
                value_to_write = &record[record_id].value2;
                version_to_update = &record[record_id].version2;
            }
        }
        else
        {
            /* Version Write */
            value_to_write = &version[write_loc].value;
            version_to_update = &version[write_loc].version;
        }
    }
    /* broadcast the value to write */
    value_to_write = reinterpret_cast<ValueType *>(
        __shfl_sync(all_lanes_mask, reinterpret_cast<uintptr_t>(value_to_write), leader_lane_id));

    /* write to the value */
    for (int base = 0; base < length; base += 32)
    {
        if (lane_id + base < length)
        {
            reinterpret_cast<uint32_t *>(value_to_write)[offset + base + lane_id] = data;
        }
    }

    /* make sure the writes are visible to all threads before updating the version */
    __threadfence();
    __syncwarp();

    /* update the version */
    if (lane_id == leader_lane_id)
    {
        atomicExch(version_to_update, epoch);
    }
}

template<typename ValueType>
__forceinline__ __device__ void gpuWriteToTableThread(Record<ValueType> *record, Version<ValueType> *version,
    uint32_t record_id, uint32_t write_loc, uint32_t epoch, uint32_t data, uint32_t offset = 0,
    uint32_t length = (sizeof(ValueType) + sizeof(uint32_t) - 1) / sizeof(uint32_t))
{
    constexpr uint32_t leader_lane_id = 0;
    constexpr uint32_t leader_lane_mask = 1u << leader_lane_id;
    constexpr uint32_t all_lanes_mask = 0xFFFFFFFFu;

    ValueType *value_to_write;
    uint32_t *version_to_update;
    if (write_loc == loc_record_b)
    {
        /* Record B write */
        uint64_t combined_versions =
            atomicAdd(reinterpret_cast<unsigned long long *>(&record[record_id].version1), 0ull);
        uint32_t version1 = combined_versions & 0xFFFFFFFF;
        uint32_t version2 = combined_versions >> 32;
        if (version1 < version2)
        {
            /* version 2 is the latest version before this epoch (record_a) */
            /* write to version 1 */
            value_to_write = &record[record_id].value1;
            version_to_update = &record[record_id].version1;
        }
        else
        {
            /* version 1 is the latest version before this epoch (record_a) */
            /* write to version 2 */
            value_to_write = &record[record_id].value2;
            version_to_update = &record[record_id].version2;
        }
    }
    else
    {
        /* Version Write */
        value_to_write = &version[write_loc].value;
        version_to_update = &version[write_loc].version;
    }

    /* write to the value */
    for (int i = 0; i < length; ++i)
    {
        reinterpret_cast<uint32_t *>(value_to_write)[offset + i] = data;
    }

    /* make sure the writes are visible to all threads before updating the version */
    __threadfence();

    /* update the version */
    atomicExch(version_to_update, epoch);
}

} // namespace epic

#endif // EPIC_CUDA_AVAILABLE

#endif // GPU_STORAGE_CUH
