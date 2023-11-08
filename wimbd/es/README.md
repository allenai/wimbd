Useful functions wrapping around Elasticsearch
==============================================

Connect to the server with a read-only account
----------------------------------------------

```Python
from wimbd.es import es_init
es = es_init()
```

Find out which indices exist (with other information about the index)
---------------------------------------------------------------------
```Python
from wimbd.es import get_indices

# This returns all indices, along with their total document counts.
print(get_indices())

# This also returns elasticsearch mapping information.
print(get_indices(return_mapping=True))
```

At the moment, this will return the following indices:
```Python
{'re_pile': {'docs.count': '211036967'},
 'laion2b-multi-2': {'docs.count': '1133101637'},
 'laion2b-multi-1': {'docs.count': '1133101297'},
 'test-index': {'docs.count': '1000'},
 'openwebtext': {'docs.count': '8013769'},
 's2orc-abstracts': {'docs.count': '10101555'},
 're_laion2b-en-1': {'docs.count': '1161075864'},
 're_laion2b-en-2': {'docs.count': '1161076588'},
 'c4': {'docs.count': '1074273501'},
 'laion1b-nolang': {'docs.count': '1271703630'},
 're_oscar': {'docs.count': '431992659'}}
```
 
Search over one index
---------------------

Search for one or more terms, or sequences of terms (phrases). When you search for
a sequence of terms, their exact order is matched. 

```Python
from wimbd.es import count_documents_containing_phrases

# Count the number of documents containing the term "legal".
count_documents_containing_phrases("test-index", "legal")  # single term

# Count the number of documents containing the term "legal" OR the term "license".
count_documents_containing_phrases("test-index", ["legal", "license"])  # list of terms

# Count the number of documents containing the phrase "terms of use" OR "legally binding".
count_documents_containing_phrases("test-index", ["terms of use", "legally binding"])  # list of word sequences

# Count the number of documents containing both `winter` AND `spring` in the text.
count_documents_containing_phrases("test-index", ["winter", "spring"], all_phrases=True)
```

If you want to actually inspect the documents, you can use `get_documents_containing_phrases` with the same queries as above instead.

```Python
from wimbd.es import get_documents_containing_phrases

# Get documents containing the term "legal".
get_documents_containing_phrases("test-index", "legal")  # single term

# Specify the number of documents to return using `num_documents`. Default is 10.
# Get documents containing the term "legal" OR the term "license".
get_documents_containing_phrases("test-index", ["legal", "license"], num_documents=50)  # list of terms

# Get documents containing the phrase "terms of use" OR "legally binding".
get_documents_containing_phrases("test-index", ["terms of use", "legally binding"])  # list of word sequences

# Get documents containing both `winter` AND `spring` in the text.
get_documents_containing_phrases("test-index", ["winter", "spring"], all_phrases=True)
```

Get total number of a term's occurrences (as opposed to document counts)
------------------------------------------------------------------------
```Python
from wimbd.es import count_total_occurrences_of_unigrams

count_total_occurrences_of_unigrams("test-index", ["legal", "license"])
```

Search over multiple indices
----------------------------

Because LAION has more documents than can fit into one Elastic Search index, it is split over multiple indices.
Fortunately, you can query more than one index at a time.

```Python
from wimbd.es import count_documents_containing_phrases

count_documents_containing_phrases("re_laion2b-en-*", "the woman")
```
