import subprocess

import numpy as np
import pandas as pd
from datasets import load_dataset
from datasets.builder import DatasetGenerationError, ManualDownloadError
from tqdm import tqdm


def main():

    df = pd.read_csv('resources/p3_datasets.tsv', sep='\t', names=['dataset', 'sub_dataset', 'fields'])
    df = df.replace({np.nan: None})
    df['fields'] = df['fields'].str[1:-1].str.replace('\'', '').str.replace(' ', '')
    df['sub_dataset'] = df['sub_dataset'].str.replace(' ', '')

    df = df[df['fields'].str.count(',') >= 1]
    print(len(df))

    
    for corpus in ['c4', 'openwebtext', 're_oscar', 're_pile']:
        for ind, row in tqdm(df.iterrows()):
            print(ind, row.dataset, row.sub_dataset)
            try:
                dataset = load_dataset(row.dataset, row.sub_dataset, cache_dir='/mnt/tank3/.cache/huggingface/datasets/')
            except FileNotFoundError:
                print('file not found:', row.dataset, row.sub_dataset)
                continue
            except ConnectionError:
                print('connection error:', row.dataset, row.sub_dataset)
                continue
            except DatasetGenerationError:
                print('dataset generation error:', row.dataset, row.sub_dataset)
                continue
            except ManualDownloadError:
                print('dataset manual downloade error:', row.dataset, row.sub_dataset)
                continue
            keys = dataset.keys()
            for key in keys:
                if 'test' in key:
                    if len(dataset[key]) > 25000:
                            continue
                    fields = row.fields
                    print(f"python wimbd/es/corpus_contamination.py --corpus {row.dataset} --sub_corpus {row.sub_dataset} --index {corpus} --split {key} --fields {fields}")
                    try:
                        # server
                        if row.sub_dataset:
                            subprocess.run(["python", f"/home/yanaie/wimbd/wimbd/es/corpus_contamination.py",  "--corpus", row.dataset, "--sub_corpus", row.sub_dataset, "--index", corpus, "--split", key, "--fields", fields])
                        else:
                            subprocess.run(["python", f"/home/yanaie/wimbd/wimbd/es/corpus_contamination.py",  "--corpus", row.dataset, "--index", corpus, "--split", key, "--fields", fields])
                    except:
                        print("failed running this scipt")
                        continue

if __name__ == "__main__":
    main()
