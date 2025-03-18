CC_TYPES=("default" "tictoc" "mvcc")
NUM_WAREHOUSES=("1" "2" "4" "8" "16" "32" "64")
OUTPUT_DIR=$1

mkdir -p "$OUTPUT_DIR"
START_TIME=$SECOND

for cc_type in "${CC_TYPES[@]}"; do
    for num_warehouses in "${NUM_WAREHOUSES[@]}"; do
                for repeat in {1..3}; do
                    echo "Running TPCC with cc_type=${cc_type}, num_warehouses=${num_warehouses}"
                    filename="${OUTPUT_DIR}/tpcc_${cc_type}_num_warehouses${num_warehouses}_${repeat}.txt"
                    ./tpcc_bench -t32 -m2 "-i${cc_type}" -g -x "-w${num_warehouses}" > "$filename"
                done
                ELAPSED_TIME=$((SECONDS - START_TIME))
                echo "Current Runtime ${ELAPSED_TIME} s"
    done
done
