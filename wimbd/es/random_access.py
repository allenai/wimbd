import h5py
import numpy as np

from wimbd.es import es_init


def random_documents(index_name: str, h5_path: str, n: int):
    es = es_init()
    indices = [
        name for name in es.indices.get(index="*").keys() if not name.startswith(".")
    ]

    assert index_name in indices, "input index is not supported"

    f = h5py.File(f"{h5_path}/ids.h5", "r")
    ds = f["ids"]

    rand_doc_ids = np.random.randint(0, len(ds), n)

    docs = [ds[x].decode() for x in rand_doc_ids]

    es_docs = es.mget(index="laion2b-en-2", ids=docs)["docs"]
    return es_docs
