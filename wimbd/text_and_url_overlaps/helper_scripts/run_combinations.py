import os
import argparse

CONJUNCTION = "_and_"
FILE_EXTENSION = ".txt"

# https://docs.python.org/3/library/itertools.html#itertools-recipes
from itertools import chain, combinations
def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

def main():
    parse = argparse.ArgumentParser("")

    parse.add_argument("--datasets", type=str, nargs='+')
    parse.add_argument("--sorted_uniq_dir", type=str)


    args = parse.parse_args()

    datasets = args.datasets
    sorted_uniq_dir = args.sorted_uniq_dir 

    # iterate through all subsets, smallest to largest
    for combination in powerset(datasets):
        # The one dataset hash lists need to be already computed
        if len(combination) < 2:
            continue

        # Take an existing combination and a single dataset hash list and find the overlap between them
        file1 = os.path.join(sorted_uniq_dir, CONJUNCTION.join(combination[:-1]) + FILE_EXTENSION)
        file2 = os.path.join(sorted_uniq_dir, combination[-1] + FILE_EXTENSION)
        outfile = os.path.join(sorted_uniq_dir, CONJUNCTION.join(combination[:]) + FILE_EXTENSION)
        print(f"merging: {file1}, {file2}")
        script_dir = os.path.dirname(os.path.realpath(__file__))
        os.system(f"{script_dir}/merge_and_get_overlap.sh {file1} {file2} > {outfile}")

if __name__ == "__main__":
    main()