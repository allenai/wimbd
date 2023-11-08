import argparse
import gzip
import json
import logging

import h5py
import numpy as np
from elasticsearch.helpers import parallel_bulk
from tqdm import tqdm
from wimbd.es import es_init, get_indices

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Index documents stored in jsonl.gz files"
    )
    parser.add_argument("--index", type=str, required=True)
    parser.add_argument("--id-field", type=str, required=False, default=None)
    parser.add_argument("--skip", type=int, required=False, default=0)
    parser.add_argument("--bulk-docs", type=int, required=False, default=500)
    parser.add_argument(
        "--max-doc-size", type=int, required=False, default=1024 * 1024 * 99
    )
    parser.add_argument("--text-field", type=str, required=False, default="text")
    parser.add_argument(
        "--skip-fields",
        type=str,
        required=False,
        default="",
        help="comma-separated list of fields to NOT include in the index",
    )
    parser.add_argument("--save-ids-file", type=str, required=False, default=None)
    # parser.add_argument("filename", type=str, nargs="+")
    parser.add_argument("--filenames-path", type=str, required=False)
    parser.add_argument("--num-shards", type=int, required=False, default=4)
    parser.add_argument(
        "--es-config", type=str, required=False, default="../../es_config.yml"
    )

    args = parser.parse_args()

    if args.filenames_path:
        with open(args.filenames_path) as f:
            filenames = f.readlines()
            filenames = [path.strip() for path in filenames]
    else:
        filenames = args.filename

    with open("es_index.config", "w+") as f:
        json.dump(args.__dict__, f, indent=4)

    es = es_init(args.es_config)
    index = args.index.lower()

    skip_fields = args.skip_fields.split(",")
    skip_fields = [s for s in skip_fields if s != ""]

    def make_action(filename, line_number, line):
        doc = json.loads(line)
        doc["text"] = doc.pop(args.text_field)[: args.max_doc_size]
        for field in skip_fields:
            doc.pop(field, None)
        if args.id_field is None:
            doc_id = f"{filename}-{line_number}"
        else:
            doc_id = doc.pop(args.id_field)
        return {"_source": doc, "_index": index, "_id": doc_id, "_op_type": "create"}

    assert (
        len(filenames) == 1 or args.skip == 0
    ), "You can't skip when you specify more than one file."

    if args.save_ids_file:
        with h5py.File(args.save_ids_file, "w") as f:
            dset = f.create_dataset("ids", shape=(0,), maxshape=(None,), dtype="S40")

    if index not in get_indices(es=es):
        logger.info(
            f"The index '{index}' is being created with {args.num_shards} shards"
        )
        es.indices.create(
            index=index, settings={"index.number_of_shards": args.num_shards}
        )
    else:
        logger.info(f"'{index}' already exists. Indexing more documents into it...")

    for filename in filenames:
        with gzip.open(filename, "rt", encoding="UTF8") as f:
            actions = (
                make_action(filename, line_number, line)
                for line_number, line in enumerate(f)
            )
            actions = (
                action
                for action in actions
                if action["_source"]["text"] is not None
                and len(action["_source"]["text"]) > 0
            )
            if args.skip > 0:
                import itertools

                actions = itertools.islice(actions, args.skip, None)
            results = parallel_bulk(
                es,
                actions,
                ignore_status={409},
                # max_retries=10,
                thread_count=32,
                raise_on_error=False,
                chunk_size=args.bulk_docs,
            )
            result_counts = {True: 0, False: 0}
            results_tqdm = tqdm(results, desc=f"Processing {filename}")
            result_ids = []
            for result in results_tqdm:
                status = result[1]["create"]["status"]
                result_ids.append(result[1]["create"]["_id"])
                assert status in {201, 409}, repr(result)
                result_counts[result[0]] += 1
                results_tqdm.set_postfix(
                    {str(k): v for k, v in result_counts.items()}, refresh=False
                )

            if args.save_ids_file:
                with h5py.File(args.save_ids_file, "a") as f:
                    dset = f["ids"]
                    new_size = dset.shape[0] + len(result_ids)
                    dset.resize((new_size,))
                    dset[-len(result_ids) :] = np.array(result_ids, dtype="S40")


if __name__ == "__main__":
    main()
