import gzip
import json
import sys
import tempfile
from datetime import datetime
from functools import cache
from typing import Tuple
from urllib.parse import urlparse

from bettermap import map_per_process
from ftfy import ftfy
from google.cloud import storage
from tensorflow.python.data import TFRecordDataset
import tensorflow as tf
from tqdm import tqdm

@cache
def storage_client():
    return storage.Client()

def parse_gs_url(url: str) -> Tuple[storage.Bucket, str]:
    r = urlparse(url)
    assert r.scheme == "gs"
    return storage_client().bucket(r.netloc), r.path[1:]

def input_name_to_output_name(s: str) -> str:
    s = s.replace("tfrecord-", "")
    s = s[:s.index("-of-")]
    s += ".json.gz"
    return s

def blob_to_name(o: storage.Blob) -> str:
    return o.name.rsplit("/", 1)[-1]

def main():
    input_bucket, input_prefix = parse_gs_url(sys.argv[1])
    output_bucket, output_prefix = parse_gs_url(sys.argv[2])
    if len(output_prefix) > 0 and output_prefix[-1] != "/":
        output_prefix += "/"

    existing_files = set(
        b.name.rsplit("/", 1)[-1]
        for b in tqdm(output_bucket.list_blobs(prefix=output_prefix), desc="Finding files to skip")
    )

    files_to_process = [
        b
        for b in tqdm(input_bucket.list_blobs(prefix=input_prefix), desc="Finding files to not skip")
        if ".tfrecord-" in blob_to_name(b) and input_name_to_output_name(blob_to_name(b)) not in existing_files
    ]

    def process_object(o: storage.Blob) -> int:
        input_filename = blob_to_name(o)
        output_filename = input_name_to_output_name(input_filename)
        language = input_filename.split("-", 2)[1]
        document_count = 0
        with tempfile.NamedTemporaryFile("wb+", prefix="mC4-", suffix=".tfrecords") as f:
            o.download_to_file(f)
            f.file.flush()
            with tempfile.TemporaryFile("wb+", prefix="mC4-", suffix=".json.gz") as output_file:
                with gzip.GzipFile(fileobj=output_file, mode="wb+") as gzip_file:
                    for index, record in enumerate(TFRecordDataset(f.name)):
                        def get_feature(feature: str):
                            return example.features.feature[feature].bytes_list.value[0].decode()
                        example = tf.train.Example.FromString(record.numpy())
                        line = {
                            "source": "mc4",
                            "id": f"{output_filename[:-8]}-{index}",
                            "text": ftfy(get_feature('text')),
                            "added": datetime.utcnow().isoformat(),
                            "timestamp": get_feature('timestamp'),
                            "metadata": {
                                "url": get_feature("url")
                            },
                            "lang": {language: 1.0}
                        }
                        gzip_file.write((json.dumps(line) + "\n").encode("utf8"))
                        document_count += 1
                output_blob = output_bucket.blob(output_prefix + output_filename)
                output_blob.upload_from_file(output_file, rewind=True)
        print(f"Finished processing {input_filename} with {document_count} documents.")
        return document_count

    #map_fn = map
    map_fn = map_per_process
    documents_processed = sum(map_fn(process_object, files_to_process))
    print(f"Processed {documents_processed} documents")

if __name__ == "__main__":
    main()