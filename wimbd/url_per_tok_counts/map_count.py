import argparse
from urllib.parse import urlparse
from uniseg.wordbreak import words

from wimbd.utils.utils import read_json_gz_file


def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str)

    args = parse.parse_args()

    data = read_json_gz_file(args.in_file)

    for row in data:
        tokenized_words = list(words(row['text']))
        word_count = len([x for x in tokenized_words if x != ' '])
        #print(urlparse(row['metadata']['url']).netloc, word_count)
        print(urlparse(row['url']).netloc, word_count)
	

if __name__ == "__main__":
    main()

