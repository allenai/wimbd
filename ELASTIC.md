How to use Elastic Search
=========================

Connect to the server with a read-only account
----------------------------------------------

```Python
from elasticsearch import Elasticsearch
es = Elasticsearch(
         cloud_id="TODO",
         api_key="TODO",
         retry_on_timeout=True,
         http_compress=True)
```

Find out which indices exist
----------------------------
```Python

indices = [name for name in es.indices.get("*").keys() if not name.startswith(".")]

# Or in the newer es version

indices = [name for name in es.indices.get(index="*").keys() if not name.startswith(".")]
```

At the moment, this will give the following indices:
 * **c4**: All of C4, including the "en", "en.noblocklist", and "en.noclean" subsets.
 * **laion1b-nolang**: Portion of LAION that doesn't have a detected language
 * **laion2b-multi-1**: Part one of LAION with a detected language other than English
 * **laion2b-multi-2**: Part two of LAION with a detected language other than English
 * **metrics-endpoint.metadata_current_default**: Internal ES stuff
 * **openwebtext**: Is what it says.
 * **re_laion2b-en-1**: Part one of LAION with a detected language of English
 * **re_laion2b-en-2**: Part two of LAION with a detected language of English
 * **re_oscar**: [OSCAR](https://oscar-project.org).
 * **re_pile**: [PILE](https://pile.eleuther.ai)
 * **s2orc-abstracts**: Abstracts from [S2ORC](https://github.com/allenai/s2orc). The URLs in this are Semantic Scholar URLs generated from the paper IDs.
 * **search-test**: Test index that's empty. I keep this around to look at the default mappings from time to time.
 
Search over one index
---------------------

This searches for the word "water" in the OSCAR dataset.

```Python
es.search(index="re_oscar", body={
    "query": {
        "match": {
            "text": "water"
        }
     }
})
```

Search over multiple indices
----------------------------

Because LAION has more documents than can fit into one Elastic Search index, it is split over multiple indices.
Fortunately, you can query more than one index at a time. Here is an example for searching for the word "water" in all of LAION-2B-en:

```Python
es.search(index="re_laion2b-en-*", body={
    "query": {
        "match": {
            "text": "water"
        }
    }
})
```

Search over C4 subsets
----------------------

Because C4 is so big, and the subsets have considerable overlap, we didn't want to make one index per subset.
Instead, every document has a "subset" field that can be queried.
Here is how you search for "water" only in the "en" subset:

```Python
es.search(index="c4", body={
    "query": {
        "bool": {
            "must": {
                "match": {"text": "water"}
             },
             "filter": {
                 "term": {"subset": "en"}
             }
         }
     }
})
```


Getting documents when you already have a document ID
-----------------------------------------------------

When you already have a document ID, you don't need to search.
You can just retrieve the document.
It's much faster.
This is how you do it:

```Python
es.get("re_laion2b-en-2", doc_id)
```

Document IDs are usually derived from the source data, if the source data has a reasonable ID that we can use.
If they can't be derived from the source data, they are derived from the JSON file and line number that the document came from.
 * LAION has an "ID" field in the source data, but it's not a unique identifier. Many LAION documents share the same ID, so I didn't use that field.
 * C4 document IDs are hashes of the URL. See below.


C4 document IDs are hashes of the URL
-------------------------------------

I wish we could just use the URL as document ids, but they are too long.
So instead, we use a hash of the URL as document ID.
This is how you turn a URL into a document ID:

```Python
import hashlib
encoded_url = url.strip().encode("UTF8")
doc_id = hashlib.blake2b(encoded_url).hexdigest()[:512]
```

Access random documents
------------------------
```Python
import h5py
import random


dt = h5py.string_dtype(encoding='utf-8')
f = h5py.File("ids.h5", "r")
ds = f["ids"]

random_doc_id = random.choice(ds).decode()


es.get(index="laion2b-en-2", id=random_doc_id)
```