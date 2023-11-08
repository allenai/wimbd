import argparse
from wimbd.utils.utils import read_jsonl_smart_open_file
import hashlib

def main():
    """
    Get MD5 hashes of text in jsonl.gz file and print to stdout
    """
    data = read_jsonl_smart_open_file(args.in_file)
    
    for row in data:
        if row['text'] is None:
            print('None')
            continue
        h = hashlib.md5()
        h.update(row['text'].encode())
        print(h.hexdigest())
	

if __name__ == "__main__":
    parse = argparse.ArgumentParser("")
    parse.add_argument("--in_file", type=str)
    args = parse.parse_args()
    main()

