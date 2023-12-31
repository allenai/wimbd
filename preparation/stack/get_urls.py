import argparse
import json
from typing import Dict, List

import tqdm

from auth_utils import fs_auth


def list_stack_files(languages: List[str], lang_idx: int, total_files: int, version: str = "v1.1"):
    """
    Get the list of parquet files for all languages in The Stack.
    """
    fs = fs_auth()
    lang_files: Dict[str, List[str]] = {}
    try:
        with tqdm.tqdm(total=len(languages)) as pbar:
            for idx, lang in enumerate(languages):
                if idx >= lang_idx:
                    num_data_file = 1
                    pbar.set_description(f"Total files: {total_files}. Getting urls for {lang}")
                    # HuggingFace parquet API does not allow listing parquet files for large
                    # datasets for some reason, so we construct urls by pattern matching.
                    # Also, we specify version tag instead of main, for reproducibility.

                    url = (
                        "https://huggingface.co/datasets/bigcode/the-stack/"
                        f"resolve/{version}/data/{lang}/train-00000-of-00001.parquet"
                    )

                    while not fs.exists(url):
                        #lang_files[lang].append(url)
                        url = (
                            "https://huggingface.co/datasets/bigcode/the-stack/"
                            f"resolve/{version}/data/{lang}/train-00000-of-{(num_data_file + 1):05d}.parquet"
                        )
                        num_data_file += 1

                    lang_files[lang] = []
                    for i in range(num_data_file):
                        url = (
                            "https://huggingface.co/datasets/bigcode/the-stack/"
                            f"resolve/{version}/data/{lang}/train-{i:05d}-of-{num_data_file:05d}.parquet"
                        )
                        lang_files[lang].append(url)
                        total_files += 1
                pbar.update(1)
    except KeyboardInterrupt:
        lang_files.pop(lang, None)  # remove last lang as it may be incomplete
        return lang_files
    return lang_files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get URLs to be downloaded.")
    # parser.add_argument("--lang-list-file", type=str, required=True)
    parser.add_argument("--output-file", type=str, required=True)
    parser.add_argument("--version", type=str, required=False, default="v1.1")

    args = parser.parse_args()

    fs = fs_auth()
    lang_list_file = (
        "https://huggingface.co/datasets/bigcode/the-stack/resolve/"
        f"{args.version}/programming-languages.json"
    )
    with fs.open(lang_list_file) as f:
        langs = json.load(f)
        langs = [k.lower().replace(" ", "-").replace("#", "-sharp") for k in langs.keys()]

    lang_idx = 0
    lang_urls: Dict[str, List[str]] = {}
    try:
        with open(args.output_file, "r+") as f:
            for url in f.readlines():
                url = url.rstrip("\n")
                lang = url.split("/")[-2]
                #if lang.endswith("#"):
                #    lang = lang.replace("#", "-sharp")
                if lang in lang_urls:
                    lang_urls[lang].append(url)
                else:
                    lang_urls[lang] = [url]
        lang_idx = len(lang_urls)
        if lang_idx <= len(langs):
            raise FileNotFoundError
    except FileNotFoundError:
        urls = list_stack_files(langs, lang_idx, sum([len(v) for v in lang_urls.values()]), args.version)
        lang_urls.update(urls)

    with open(args.output_file, "w+") as f:
        for _, urls in lang_urls.items():
            for url in urls:
                f.write(url + "\n")
