from tqdm.autonotebook import tqdm
import json
import argparse
import os
import multiprocessing as mp

file_extension = ".txt"

def get_counts(args):
        dataset, sorted_uniq_count_dir = args
        filename = dataset + file_extension
        path = os.path.join(sorted_uniq_count_dir, filename)
        dataset_counts = {
            "duplicates" : 0,
            "total" : 0,
            "uniq_duplicates": 0,
            "uniq_total": 0
        }
        with open(path) as fin:
            for line in fin:
                line = line.strip().split(' ')
                count = line[0]
                count = int(count)
                if count > 1:
                    dataset_counts['duplicates'] += count
                    dataset_counts['uniq_duplicates'] += 1
                dataset_counts['total'] += count
                dataset_counts['uniq_total'] += 1
        return dataset, dataset_counts

def main(args):
    datasets = args.datasets
    sorted_uniq_count_dir = args.sorted_uniq_count_dir

    combined_numbers = {}
    dataset_args = [(dataset, sorted_uniq_count_dir) for dataset in datasets]
    with mp.Pool() as pool:
        for dataset, dataset_counts in tqdm(pool.imap_unordered(get_counts, dataset_args), total=len(datasets)):
            combined_numbers[dataset] = dataset_counts

    with open(args.out_file, 'w') as fout:
        json.dump(combined_numbers, fout, indent=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets", type=str, nargs='+')
    parser.add_argument("--out_file", type=str)
    parser.add_argument("--sorted_uniq_count_dir", type=str)
    args = parser.parse_args()


    main(args)