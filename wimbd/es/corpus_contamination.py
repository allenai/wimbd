import argparse

import wandb
from datasets import load_dataset
from wimbd.es import (count_documents_for_each_phrase, es_init)
from pathlib import Path


def log_wandb(args):

    multiple_fields = len(args.fields.split(",")) > 1
    config = dict(
        corpus=args.corpus,
        sub_corpus=args.sub_corpus,
        split=args.split,
        index=args.index,
        field=args.fields,
        multiple_fields=multiple_fields,
    )

    wandb.init(    
        name=f'contamination_{args.corpus}_{args.sub_corpus}_{args.split}_{args.fields}_{args.index}',
        project="wimbd",
        config=config,
    )


def contamination_percentage(index_name: str, corpus_name: str, sub_corpus_name: str = None, split: str = "train", field: str = "text"):
    
    path = (Path(__file__).parent / ".." / "..").resolve() / "es_config.yml"
    es = es_init(path, timeout=180)
    
    dataset = load_dataset(corpus_name, sub_corpus_name, cache_dir='/mnt/tank3/.cache/huggingface/datasets/')
    string_list = dataset[split][field]
    wandb.run.summary['n'] = len(string_list)

    counts = sum([x > 0 for x in count_documents_for_each_phrase(index_name, string_list, batch_size=60, es=es)])
    wandb.run.summary['count'] = counts
    wandb.run.summary['percentage'] = counts / len(string_list)

    return counts / len(string_list)


def paired_contamination_percentage(index_name: str, corpus_name: str, sub_corpus_name: str = None, split: str = "train", fields: list[str] = None):

    path = (Path(__file__).parent / ".." / "..").resolve() / "es_config.yml"
    es = es_init(path, timeout=180)
    
    dataset = load_dataset(corpus_name, sub_corpus_name, cache_dir='/mnt/tank3/.cache/huggingface/datasets/')
    string_list = list(zip(*[dataset[split][x] for x in fields]))
    string_list = [list(x) for x in string_list]
    wandb.run.summary['n'] = len(string_list)

    counts = sum([x > 0 for x in count_documents_for_each_phrase(index_name, string_list, batch_size=60, es=es, all_phrases=True)])
    wandb.run.summary['count'] = counts
    wandb.run.summary['percentage'] = counts / len(string_list)

    return counts / len(string_list)
    

def main():

    parse = argparse.ArgumentParser("")
    parse.add_argument("--corpus", type=str)
    parse.add_argument("--sub_corpus", type=str, default=None)
    parse.add_argument("--index", type=str)
    parse.add_argument("--fields", type=str)
    parse.add_argument("--split", type=str)
    parse.add_argument("--option", type=int, default=-1)

    args = parse.parse_args()

    log_wandb(args)

    if len(args.fields.split(',')) > 1:
        c = paired_contamination_percentage(args.index, args.corpus, args.sub_corpus, args.split, args.fields.split(','))
    else:
        c = contamination_percentage(args.index, args.corpus, args.sub_corpus, args.split, args.fields)
    print(c)


if __name__ == "__main__":
    """
    Usage example:
    
    ```python wimbd/es/corpus_contamination.py --corpus glue --sub_corpus rte --index c4 --split train --field sentence1```

    """
    main()
