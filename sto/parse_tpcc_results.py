import csv
import logging
import os
import re
import sys

import numpy as np

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')
root = logging.getLogger()
handler = logging.FileHandler('parse_experiment.log', 'w+')
handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s'))
root.addHandler(handler)

num_warehouses = ["1", "2", "4", "8", "16", "32", "64"]
cc_types = ["default", "tictoc", "mvcc"]
repeat = 3

REAL_THROUGHPUT_PATTERN = re.compile(r"Real throughput: (\d+\.\d+) MTxns/sec")


def main(logs_dir, output_dir):
    logging.info(f"Parsing logs from directory {logs_dir} and saving to {output_dir}")
    if not os.path.exists(output_dir):
        logging.info("Creating output directory")
        os.makedirs(output_dir)
    logging.info(f"cc_types: {cc_types}")
    logging.info(f"num_warehouses: {num_warehouses}")
    logging.info(f"repeat: {repeat}")

    for cc_type in cc_types:
        logging.info(f"Parsing {cc_type} logs")
        table = [['num_warehouses', 'throughput']]
        for num_w in num_warehouses:
            throughput = []
            for i in range(1, repeat + 1):
                filename = f"tpcc_{cc_type}_num_warehouses{num_w}_{i}.txt"
                filepath = os.path.join(logs_dir, filename)
                logging.info(f"Parsing {filename}")
                with open(filepath, 'r') as f:
                    lines = f.readlines()
                    logging.debug(f"Found {len(lines)} lines")
                    for line in lines:
                        match = REAL_THROUGHPUT_PATTERN.match(line)
                        if match:
                            throughput.append(float(match.group(1)))
                            break
            logging.debug(f"Found {len(throughput)} throughput values: {throughput}")
            row = [num_w, np.mean(throughput)]
            table.append(row)
        logging.info(f"Saving table for tpcc {cc_type}")
        csv_filename = f"tpcc_{cc_type}.csv"
        with open(os.path.join(output_dir, csv_filename), 'w') as f:
            writer = csv.writer(f)
            writer.writerows(table)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <directory_path> <output_path>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
