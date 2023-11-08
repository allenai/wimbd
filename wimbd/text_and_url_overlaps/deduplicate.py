# A script to deduplicate a set of documents with themselves
# Usage: decontaminate.sh --in_files <file1> <file2> ... --out_dir <dir> --tmp_dir <dir>
# The script will generate a blocklist of text hashes that appear in in_files and store this in the tmp_dir
# It will remove all documents matching the blocklist except the first occurance from in_files and store the result in out_dir

import argparse
import os
import subprocess

def main(args):
    if  args.blocklist is not None:
        blockfile = args.blocklist
    else:
        blockfile = os.path.join(args.tmp_dir, 'duplicates_hashes.txt')
        # Generate blocklist
        cmd = f"$TEXT_AND_URL_OVERLAPS/helper_scripts/run_get_text_hashes.sh {' '.join(args.in_files)} | $TEXT_AND_URL_OVERLAPS/helper_scripts/get_duplicate_keys.sh > {blockfile}"
        subprocess.run(cmd, shell=True)

    # Decontaminate
    if not args.build_blocklist_only:
        cmd = f"python $TEXT_AND_URL_OVERLAPS/helper_scripts/deduplicate_by_hash.py --shards {' '.join(args.in_files)} --blocklist {blockfile} --outpath {args.out_dir} --just_write_attributes --keep_first_occurrence"
        subprocess.run(cmd, shell=True)


if __name__ in "__main__":
    parser = argparse.ArgumentParser("")
    parser.add_argument("--in_files", type=str, nargs='+')
    parser.add_argument("--out_dir", type=str)
    parser.add_argument("--tmp_dir", type=str)
    parser.add_argument("--blocklist", type=str, default=None)
    parser.add_argument("--build_blocklist_only", action='store_true')
    args = parser.parse_args()

    # blocklist and in_files1 are mutually exclusive
    assert (args.blocklist is None) != (args.in_files is None), "Must specify either blocklist or in_files"

    main(args)