
set -Eeuo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PYTHON=$(which python | head -1)

parallel "$PYTHON $SCRIPT_DIR/map_count.py --in_file {} --classifier regex" ::: "$@" | $PYTHON $SCRIPT_DIR/reduce_count.py