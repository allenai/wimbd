import argparse
from glob import glob
import gzip
from tqdm import tqdm


def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_folder", type=str)
    parse.add_argument("--n", type=int, default=5)
    
    args = parse.parse_args()

    # iterate over files in the folder, and split each file (json.gz) into n files while adding a suffix at the end of the name based on the index
    for file in tqdm(glob(args.in_folder + "/*.jsonl.gz")):
        with gzip.open(file, 'rt') as f:
            data = f.readlines()

        n = args.n
        chunks = len(data) // n
        for i in range(n - 1):
            file_split = file.split(".")[0] + f"_{i}.json.gz"
            print(i*chunks, (i+1)*chunks)
            with gzip.open(file_split, 'wt') as f:
                f.writelines(data[i*chunks:(i+1)*chunks])
        i = n - 1
        file_split = file.split(".")[0] + f"_{i}.json.gz"
        print(i*chunks, (i+1)*chunks)
        with gzip.open(file_split, 'wt') as f:
            f.writelines(data[i*chunks:])

if __name__ == "__main__":
    main()


