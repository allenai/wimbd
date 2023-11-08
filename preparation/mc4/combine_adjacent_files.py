"""mC4 comes in many small files. This consolidates the files by a factor of 4."""
import collections
import sys
from typing import Tuple, List, Dict
from urllib.parse import urlparse

import more_itertools
from google.cloud import storage
from tqdm import tqdm


def parse_gs_url(storage_client, url: str) -> Tuple[storage.Bucket, str]:
    r = urlparse(url)
    assert r.scheme == "gs"
    return storage_client.bucket(r.netloc), r.path[1:]


def blob_to_name(o: storage.Blob) -> str:
    return o.name.rsplit("/", 1)[-1]


def parse_blob(blob):
    name = blob_to_name(blob)
    components = name.split(".")
    return components[0], int(components[1])


def main():
    storage_client = storage.Client()
    input_bucket, input_prefix = parse_gs_url(storage_client, sys.argv[1])
    output_bucket, output_prefix = parse_gs_url(storage_client, sys.argv[2])
    if len(output_prefix) > 0 and output_prefix[-1] != "/":
        output_prefix += "/"

    existing_files = set(
        b.name
        for b in tqdm(output_bucket.list_blobs(prefix=output_prefix), desc="Finding files to skip")
    )

    files_to_process = [
        b
        for b in tqdm(input_bucket.list_blobs(prefix=input_prefix), desc="Finding files to not skip")
        if blob_to_name(b).endswith(".json.gz")
    ]
    files_to_process.sort(key=blob_to_name)

    prefix_to_blobs: Dict[str, List[storage.Blob]] = collections.defaultdict(list)
    for blob in files_to_process:
        prefix = parse_blob(blob)[0]
        prefix_to_blobs[prefix].append(blob)

    # merges need to be URLs, because Google Storage is stupid and can't be used in multiple processes
    for prefix, blobs in tqdm(prefix_to_blobs.items(), desc="Merging files"):
        for chunk in more_itertools.chunked(blobs, 4):
            output_name = output_prefix
            output_name += parse_blob(chunk[0])[0]
            output_name += "."
            output_name += "-".join(f"{parse_blob(blob)[1]:05}" for blob in chunk)
            output_name += ".json.gz"
            if output_name in existing_files:
                continue

            output_blob = output_bucket.blob(output_name)
            output_blob.compose(chunk)


if __name__ == "__main__":
    main()
