#!/bin/bash
set -Eeuo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PYTHON=$(which python | head -1)

DATASET=$1

echo $DATASET

parallel --progress --tmpdir="/mnt/tank3/tmp" "$PYTHON $SCRIPT_DIR/map_unique_lengths.py --dataset=$DATASET --in_file {}" ::: "$@" | $PYTHON $SCRIPT_DIR/reduce_count.py
