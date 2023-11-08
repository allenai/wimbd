import argparse
import os
from urllib.parse import urlparse

from wimbd.utils.utils import read_json_gz_file


def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str)

    args = parse.parse_args()

    data = read_json_gz_file(args.in_file)

    for row in data:
        path = urlparse(row['url']).path
        suffix = os.path.splitext(path)[1][1:]
        if suffix.lower() not in ['jpg', 'png', 'jpeg']:
            suffix = 'other'
        print(suffix)


if __name__ == "__main__":
    main()
