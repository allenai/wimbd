#!/bin/bash

shards=$1
outfile=$2
mode_examples=$3
counts_file=$4
k=$5

# this gets the directory of the script, even if it's called from another directory
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

python $SCRIPT_DIR/get_examples.py --k $k --data_shards $shards  --outfile $outfile $mode_examples <(head -n $k $counts_file)