import random

from wimbd.es import es_init


def random_access(index: str, path_to_ids: str):
    es = es_init()
    with h5py.File(path_to_ids, "r") as f:
        ds = f["ids"]
        random_doc_id = random.choice(ds).decode()
    return es.get(index=index, id=random_doc_id)
