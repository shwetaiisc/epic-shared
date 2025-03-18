ALPHAS=("0.0" "0.2" "0.4" "0.6" "0.8" "0.9" "0.95" "0.99")
READ_TYPES=("full")
CC_TYPES=("default" "tictoc" "mvcc")
YCSB_BENCH=("A" "B" "C" "F")
OUTPUT_DIR=$1

mkdir -p "$OUTPUT_DIR"
START_TIME=$SECOND

for cc_type in "${CC_TYPES[@]}"; do
    for read_type in "${READ_TYPES[@]}"; do
        for ycsb_bench in "${YCSB_BENCH[@]}"; do
            for alpha in "${ALPHAS[@]}"; do
                for repeat in {1..3}; do
                    echo "Running YCSB with alpha=${alpha}, read_type=${read_type}, cc_type=${cc_type}, ycsb_bench=${ycsb_bench}"
                    filename="${OUTPUT_DIR}/${cc_type}_${read_type}_${alpha}_YCSB${ycsb_bench}_${repeat}.txt"
                    ./ycsb_bench -t32 "-m${ycsb_bench}" "-i${cc_type}" -g -x "-s${alpha}" "-r${read_type}" > "$filename"
                done
                ELAPSED_TIME=$((SECONDS - START_TIME))
                echo "Current Runtime ${ELAPSED_TIME} s"
            done
        done
    done
done
