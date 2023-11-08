import argparse
import json
import random

from wimbd.utils.utils import read_json_gz_file


def randomly_keep(sample_fraction):
    x = random.random()
    if x < sample_fraction:
        return True
    return False


def main():
    parser = argparse.ArgumentParser("")
    parser.add_argument("--in_file", type=str)
    parser.add_argument("--sample_fraction", type=float, default=0.01)
    parser.add_argument("--key_to_sample", type=str, default='url')
    args = parser.parse_args()
    target_key = args.key_to_sample
    data = read_json_gz_file(args.in_file)
    for row in data:
        if randomly_keep(args.sample_fraction):
            sample = ''
            if target_key in row:
                sample = row[target_key]
            elif 'metadata' in row:
                sample = row['metadata'].get(target_key, '')
            print(json.dumps({target_key: sample}))


if __name__ == "__main__":
    main()
