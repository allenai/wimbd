import argparse

import tldextract
from wimbd.utils.utils import read_jsonl_file


def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str)

    args = parse.parse_args()

    data = read_jsonl_file(args.in_file)
   
    for row in data:
        #suffix = tldextract.extract(row['metadata']['url']).suffix
        suffix = tldextract.extract(row['url']).suffix
        if suffix.strip() == '': continue
        print(suffix, row['count'])
	

if __name__ == "__main__":
    main()

