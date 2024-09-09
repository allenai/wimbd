Useful functions wrapping around Elasticsearch
==============================================

Connect to the server with a read-only account
----------------------------------------------

### Get access to the indices
* Dolma index: https://forms.gle/gQN4nP4HHYGwXAis9
* Other indices: https://forms.gle/yMz7uTFhd1dKNYTk7


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

Note that the `get_indices` function won't work with the access key we provide,
since it limits the access to the ES index.
However, you can find the names of the relevant indices below.

At the moment, this will return the following indices:
```Python
{'re_pile': {'docs.count': '211036967'},
 're_laion2b_multi': {'docs.count': '2248498161'}
 'openwebtext': {'docs.count': '8013769'},
 're_laion2b-en-1': {'docs.count': '1161075864'},
 're_laion2b-en-2': {'docs.count': '1161076588'},
 'c4': {'docs.count': '1074273501'},
 're_laion2b_nolang': {'docs.count': '1271703630'},
 're_oscar': {'docs.count': '431992659'}}
```

Different Indices
-----------------
We have 3 different indices that we can make publicly available. Each contain different corpora:
* The Pile, OpenWebText, C4 and Oscar (`re_pile`, `openwebtext`, `c4`, `re_oscar`, and a bunch of fine-tuning datasets like Tulu2)
* RedPajama v1 (`redpajama-split`)
* Dolma (`docs_v1.5_2023-11-02`)


Index 1:
| Dataset                           | Index Name                            | Link                                                                        |
|-----------------------------------|-----------------------------------|-----------------------------------------------------------------------------|
| C4                                | c4                                | https://huggingface.co/datasets/allenai/c4                                  |
| OpenWebText                       | openwebtext                       | https://huggingface.co/datasets/Skylion007/openwebtext                      |
| Oscar                             | re_oscar                          | https://oscar-project.github.io/documentation/                              |
| S2ORC-abstracts                   | s2orc-abstracts                   | https://huggingface.co/datasets/sentence-transformers/s2orc                 |
| (Private - Can't share)           |                                   |                                                                             |
| Pile                              | re_pile                           | https://pile.eleuther.ai/                                                   |
| LAION-en-1                        | re_laion2b-en-1                   | https://huggingface.co/datasets/laion/relaion2B-en-research-safe            |
| LAION-en-2                        | re_laion2b-en-2                   | https://huggingface.co/datasets/laion/relaion2B-en-research-safe            |
| LAION-multi                       | re_laion2b_multi                  | https://huggingface.co/datasets/laion/relaion2B-multi-research-safe         |
| LAION-no-lang                     | re_laion2b_nolang                 | https://huggingface.co/datasets/laion/relaion1B-nolang-research             |
| FT Datasets                       |                                   |                                                                             |
| AYA                               | aya_dataset                       | https://huggingface.co/datasets/CohereForAI/aya_dataset                     |
| ShareGPT                          | sharegpt-cleaned                  | https://huggingface.co/datasets/Vtuber-plan/sharegpt-cleaned                |
| Code-Feedback                     | code-feedback                     | https://huggingface.co/datasets/m-a-p/Code-Feedback                         |
| SciRIFF-train-mix                 | sciriff-train-mix-science         | https://huggingface.co/datasets/allenai/SciRIFF-train-mix                   |
| Table-GPT                         | table-gpt-all-train               | https://huggingface.co/datasets/LipengCS/Table-GPT                          |
| WebInstructSub                    | webinstructsub                    | https://huggingface.co/datasets/TIGER-Lab/WebInstructSub                    |
| MetaMathQA                        | metamath-qa                       | https://huggingface.co/datasets/meta-math/MetaMathQA                        |
| Tulu2-SFT-Mixture                 | tulu-v2-sft-mixture               | https://huggingface.co/datasets/allenai/tulu-v2-sft-mixture                 |
| UltraFeedback Binarized           | ultrafeedback_binarized           | https://huggingface.co/datasets/HuggingFaceH4/ultrafeedback_binarized       |
| CodeFeedback-Filtered-Instruction | codefeedback-filtered-instruction | https://huggingface.co/datasets/m-a-p/CodeFeedback-Filtered-Instruction     |
| Daring-Anteater                   | daring-anteater                   | https://huggingface.co/datasets/nvidia/Daring-Anteater                      |
| No Robots                         | no_robots                         | https://huggingface.co/datasets/HuggingFaceH4/no_robots                     |
| WildChat-1M-Full                  | wildchat-1m-full-gpt4-only        | https://huggingface.co/datasets/allenai/WildChat-1M-Full                    |
| coconot-sft                       | coconot-sft                       | https://huggingface.co/datasets/ai2-adapt-dev/coconot-sft                   |
| SlimOrca                          | slimorca                          | https://huggingface.co/datasets/Open-Orca/SlimOrca                          |
| Open Assistant                    | openassistant-guanaco             | https://huggingface.co/datasets/timdettmers/openassistant-guanaco           |
| WizardLM_evol_instruct_V2_196k    | wizardlm_evol_instruct_v2_196k    | https://huggingface.co/datasets/WizardLMTeam/WizardLM_evol_instruct_V2_196k |
| NuminaMath-CoT                    | numina-math-cot                   | https://huggingface.co/datasets/AI-MO/NuminaMath-CoT                        |
| lmsys-chat-1m                     | lmsys-chat-1m                     | https://huggingface.co/datasets/lmsys/lmsys-chat-1m                         |



Index 2

| Dataset      | Index Name      | Link                                                               |
|--------------|-----------------|--------------------------------------------------------------------|
| RedPajama v1 | redpajama-split | https://huggingface.co/datasets/togethercomputer/RedPajama-Data-1T |

Index 3

| Dataset    | Index Name           | Link                                          |
|------------|----------------------|-----------------------------------------------|
| Dolma v1.5 | docs_v1.5_2023-11-02 | https://huggingface.co/datasets/allenai/dolma |
| Dolma v1.7 | docs_v1.7_2024-06-04 | https://huggingface.co/datasets/allenai/dolma |

Indices Mapping
---------------
```json
{
    'mappings': {
        'dynamic': 'false',
        'properties': {
            'date': {
                'type': 'date'
            },
            'subset': {
                'type': 'keyword', 
                'ignore_above': 256
            },
            'text': {
                'type': 'text'
            },
            'url': {
                'type': 'text'
            }
        }
    }
}
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
