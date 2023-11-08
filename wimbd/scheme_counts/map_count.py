import argparse
from urllib.parse import urlparse

from wimbd.utils.utils import read_json_gz_file


def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str)

    args = parse.parse_args()

    data = read_json_gz_file(args.in_file)
    
    for row in data:
        #print(urlparse(row['metadata']['url']).scheme)
        print(urlparse(row['url']).scheme)
	

if __name__ == "__main__":
    main()

