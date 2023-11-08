#!/bin/bash

# this gets the directory of the script, even if it's called from another directory
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# this runs the script on all the files passed in as arguments in parallel
# The output is sorted and uniqued, so that we can see how many times each hash appears
parallel "python $SCRIPT_DIR/get_text_hashes.py --in_file {}" ::: $@ | sort | uniq -c