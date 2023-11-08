import argparse
import concurrent.futures
import glob
import gzip
import itertools
import json
import logging
import os
import sys

import datasets

logger = logging.getLogger(__name__)


def chunks(iterable, start, size):
    from itertools import chain, islice

    iterator = iter(iterable)
    for first in iterator:
        yield list(
            itertools.chain(
                [first], itertools.islice(iterator, start, start + size - 1)
            )
        )


def save_dataset_as_jsonl(
    dataset_name: str,
    output_dir: str,
    batch_size: int = datasets.config.DEFAULT_MAX_BATCH_SIZE * 10,
    num_workers: int = 2,
    text_field: str = "text",
):
    # Load the dataset
    datasets.disable_caching()
    dataset = datasets.load_dataset(dataset_name, streaming=True)

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    for split in dataset:
        ds = dataset[split]
        try:
            start = max(
                [
                    int(path.split(".jsonl.gz")[0].split("_")[-1])
                    for path in glob.glob(
                        os.path.join(output_dir, split + "_*.jsonl.gz")
                    )
                ]
            )
        except ValueError:
            start = 0
        # Group the streaming examples into batches of batch_size
        batches = itertools.islice(ds, start, None, batch_size)

        if start != 0:
            logger.warning(f"Already processed until batch {start-1}. Continuing.")

        def _update_example(example: dict):
            example["text"] = example.pop(text_field)
            return example

        def _func(batch_number, batch, split):
            logger.warning(f"Processing batch number {split}-{batch_number}")
            batch_jsonl = "\n".join(
                json.dumps(_update_example(example)) for example in batch
            )

            batch_filename = os.path.join(
                output_dir, f"{split}_{batch_number}.jsonl.gz"
            )
            with gzip.open(batch_filename, "wt", encoding="utf-8") as f:
                f.write(batch_jsonl)

        # Loop through the batches and save each batch as a gzipped JSONL file

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_workers,
            thread_name_prefix="save_dataset_as_jsonl-",
        ) as executor:
            futures = []
            for i, batch in enumerate(chunks(iter(ds), start, batch_size - 1)):
                batch_number = start + i
                futures.append(executor.submit(_func, batch_number, batch, split))
            for future in concurrent.futures.as_completed(futures):
                future.result()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Save huggingface dataset to jsonl.gz files"
    )
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--output-dir", type=str, required=True)
    parser.add_argument(
        "--batch-size",
        type=int,
        required=False,
        default=datasets.config.DEFAULT_MAX_BATCH_SIZE * 10,
    )
    parser.add_argument("--num-workers", type=int, required=False, default=5)
    parser.add_argument("--text-field", type=str, required=False, default="text")

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    with open(f"{args.output_dir}/download.config", "w+") as f:
        json.dump(args.__dict__, f, indent=4)

    save_dataset_as_jsonl(
        args.dataset,
        args.output_dir,
        args.batch_size,
        args.num_workers,
        args.text_field,
    )
