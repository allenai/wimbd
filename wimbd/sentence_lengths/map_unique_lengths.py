import argparse
from nltk.tokenize import wordpunct_tokenize
from urllib.parse import urlparse

from wimbd.utils.utils import read_json_gz_file

CHAR_LENGTHS = {'c4': [192, 515, 434, 499, 322, 86],
                'mc4': [73257, 219, 186, 16629, 568, 331, 519, 2024, 526, 524, 489, 160, 4114, 697, 17189],
                'oscar': [101, 367, 108, 150, 126, 24827, 132, 106, 136, 266, 110, 141, 105, 180, 120, 138, 116, 103, 197, 182, 543, 162, 124, 154, 763, 225, 24847, 145, 114, 520, 122, 501],
                'pile': [8194, 201, 35, 100000, 71, 73],
                's2orc0': [160, 327],
                'stack': [172, 243, 182, 189, 154, 458, 192, 155, 2082, 263, 180, 1964, 158, 230, 178, 95, 2148, 84, 2058, 20000, 214, 1935, 550, 147, 1568, 109, 285, 232, 188, 3962, 88, 107, 1477, 393, 2114, 54, 4043, 2053, 208, 2116, 138, 200, 32, 197, 2153, 38, 175, 2603, 204, 2029, 60, 211, 1098, 254],
                'redpajama': [126, 133, 134, 118]}
TOK_LENGTHS = {'mc4': [99, 42, 6214, 236, 37, 71, 35, 92],
               'oscar': [64, 27, 18, 22, 4962, 74, 56, 31, 20, 105, 25, 34, 157, 248, 35, 4966, 99, 425],
               'pile': [9, 18, 20, 36],
               'redpajama': [23, 19, 54, 52, 30, 95, 32, 98, 96, 64, 66],
               'stack': [20, 33, 23, 17, 36, 10, 26, 4004, 13, 99, 39, 29, 8, 61, 454]}




def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str)
    parse.add_argument("--dataset", type=str)

    args = parse.parse_args()

    if args.dataset not in CHAR_LENGTHS and args.dataset not in TOK_LENGTHS:
        print(f'Error: dataset {args.dataset} does not have unique lengths')
        exit()

    data = read_json_gz_file(args.in_file)

    for row in data:
        text = row['text']
        if text is not None:
            to_print = ""
            if args.dataset in TOK_LENGTHS:
                tok_length = len(wordpunct_tokenize(text.strip()))
                to_print = text.replace("\n", "(newline)")

                if tok_length in TOK_LENGTHS[args.dataset]:
                    print(f'{tok_length} tok / {to_print}')

            if args.dataset in CHAR_LENGTHS:
                char_length = len(text)
                if not to_print:
                    to_print = text.replace("\n", "(newline)")

                if char_length in CHAR_LENGTHS[args.dataset]:
                    print(f'{char_length} char / {to_print}')

	

if __name__ == "__main__":
    main()

