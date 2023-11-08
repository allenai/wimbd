#!/bin/bash
set -Eeuo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PYTHON=$(which python | head -1)

parallel --eta --bar "$PYTHON $SCRIPT_DIR/map_count.py --in_file {}" ::: "$@" | $PYTHON $SCRIPT_DIR/reduce_count.py --tmpdir=/mnt/tank3/tmp
