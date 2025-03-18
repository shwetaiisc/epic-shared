#!/bin/bash

# this program is run as part of the ./run script
# to calculate averate transaction latency

if [ $# -ne 1 ]; then
    echo "Usage: calculate_latency.sh file"
    exit 1
fi

# calculate average latency and append it to the file
grep "total latency" $1 | \
    awk '{total += $22; count += $24} END { print "total latency: " total " transactions: " count " average latency: " total/count }' >> $1