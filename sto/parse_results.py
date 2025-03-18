import re
import os
import glob
import sys
import csv
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')
root = logging.getLogger()
handler = logging.FileHandler('parse_experiment.log', 'w+')
handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s'))
root.addHandler(handler)

alphas = ["0.0", "0.2", "0.4", "0.6", "0.8", "0.9", "0.95", "0.99"]
workloads = ["ycsba", "ycsbb", "ycsbc", "ycsbf"]
cc_types = ["default", "tictoc", "mvcc"]
read_types = ["field", "full"]
repeat = 3

REAL_THROUGHPUT_PATTERN = re.compile(r"Real throughput: (\d+\.\d+) MTxns/sec")

def main(logs_dir, output_dir):
    logging.info(f"Parsing logs from directory {logs_dir} and saving to {output_dir}")
    if not os.path.exists(output_dir):
        logging.info("Creating output directory")
        os.makedirs(output_dir)
    logging.info(f"alphas: {alphas}")
    logging.info(f"workloads: {workloads}")
    logging.info(f"cc_types: {cc_types}")
    logging.info(f"read_types: {read_types}")
    logging.info(f"repeat: {repeat}")

    for cc_type in cc_types:
        logging.info(f"Parsing {cc_type} logs")
        for workload in workloads:
            for read_type in read_types:
                table = [['alpha', 'throughput']]
                for alpha in alphas:
                    throughput = []
                    for i in range(1, repeat + 1):
                        uworkload = workload.upper()
                        filename = f"{cc_type}_{read_type}_{alpha}_{uworkload}_{i}.txt"
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
                    row = [float(alpha), np.mean(throughput)]
                    table.append(row)
                logging.info(f"Saving table for {cc_type}_{read_type}_{workload}")
                csv_filename = f"{cc_type}_{workload}_{read_type}.csv"
                with open(os.path.join(output_dir, csv_filename), 'w') as f:
                    writer = csv.writer(f)
                    writer.writerows(table)

                        

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <directory_path> <output_path>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])