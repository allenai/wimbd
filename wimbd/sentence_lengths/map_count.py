import argparse
from nltk.tokenize import wordpunct_tokenize
from urllib.parse import urlparse

from wimbd.utils.utils import read_json_gz_file


def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str)

    args = parse.parse_args()

    data = read_json_gz_file(args.in_file)
    
    for row in data:
        text = row['text']
        if text is not None:
            print(f'chars {len(text)}')
            print(f'tokens {len(wordpunct_tokenize(text.strip()))}')
        else:
            print('text is None')
	

if __name__ == "__main__":
    main()

