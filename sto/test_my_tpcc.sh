CC_TYPES=("default" "tictoc" "mvcc")
NUM_WAREHOUSES=("1" "64")
OUTPUT_DIR=$1

mkdir -p "$OUTPUT_DIR"
START_TIME=$SECOND

# Color codes
RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
MAGENTA="\033[35m"
CYAN="\033[36m"
GREY="\033[90m"
NC="\033[0m" # No Color

for cc_type in "${CC_TYPES[@]}"; do
    for num_warehouses in "${NUM_WAREHOUSES[@]}"; do
                for repeat in {1..1}; do
                    echo -e "${GREEN}Running TPCC with cc_type=${cc_type}, num_warehouses=${num_warehouses}${NC}"
                    filename="${OUTPUT_DIR}/tpcc_${cc_type}_num_warehouses${num_warehouses}_${repeat}.txt"
                    err_filename="${OUTPUT_DIR}/tpcc_${cc_type}_num_warehouses${num_warehouses}_${repeat}.err"
                    ./tpcc_bench -t32 -m2 "-i${cc_type}" -g -x "-w${num_warehouses}" > >(tee $filename) 2> >(tee $err_filename >&2)
                    if [ $? -ne 0 ]; then
                      echo -e "${RED}Error running TPCC with cc_type=${cc_type}, num_warehouses=${num_warehouses}${NC}"
                    fi
                done
                ELAPSED_TIME=$((SECONDS - START_TIME))
                echo -e "${BLUE}Current Runtime ${ELAPSED_TIME} s${NC}"
    done
done
