import argparse
from wimbd.utils.utils import read_json_gz_file
import hashlib
import os
import json
import gzip

def main():
    """
    Split a jsonl.gz file into train, valid, and test sets.

    This will put approximately 0.05% of data into valid and test sets, and the rest into train.

    usage with parallel:
    cd /path/to/my_dataset
    parallell --bar --eta "python /path/to/split_file.py --in_file {} --out_prefix /path/to/out/dir" ::: `find ./ -name "*.jsonl.gz"`
    """
    data = read_json_gz_file(args.in_file)
    
    #make dirs if they don't exist
    if not os.path.exists(os.path.join(args.out_prefix, os.path.dirname(args.in_file))):
        os.makedirs(os.path.join(args.out_prefix, os.path.dirname(args.in_file)))

    out_path = os.path.join(args.out_prefix, args.in_file).replace(".jsonl", "").replace(".gz", "")

    test_stream = gzip.open(out_path + ".test.jsonl.gz", "w")
    valid_stream = gzip.open(out_path + ".valid.jsonl.gz", "w")
    train_stream = gzip.open(out_path + ".train.jsonl.gz", "w")

    for row in data:
        text = row['text'] if row['text'] is not None else ""
        if id in row:
            raise ValueError("id already in row")
        row['id'] = hashlib.sha1(text.encode("utf-8")).hexdigest()

        if row["id"][:3] in {"fff", "ffe"}:
            test_stream.write(json.dumps(row) + "\n")
        elif row["id"][:3] in {"ffd", "ffc"}:
            valid_stream.write(json.dumps(row) + "\n")
        else:
            train_stream.write(json.dumps(row) + "\n")
	
    test_stream.close()
    valid_stream.close()
    train_stream.close()

if __name__ == "__main__":
    parse = argparse.ArgumentParser("")
    parse.add_argument("--in_file", type=str)
    parse.add_argument("--out_prefix", type=str)
    args = parse.parse_args()
    main()

