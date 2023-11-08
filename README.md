# What's in my big data?

[Paper](http://arxiv.org/abs/2310.20707) || [Demo](https://wimbd.apps.allenai.org) || [Artifacts](https://console.cloud.google.com/storage/browser/wimbd)

This repository contains the code for running What's In My Big Data (WIMBD), which accompanies our [recent paper](http://arxiv.org/abs/2310.20707) (with the same name).

![WIMBD overview](./resources/viz/wimbd-fig1.png)


> What is WIMBD?

WIMBD is composed of two components
1. A set of tools for analyzing and revealing the content of large-scale datasets
2. A set of analyses we apply to those datasets, using the aforementioned tools

*WIMBD tools* consist of two parts:

1. Count
2. Search

The count follows a map-reduce functionality, which divides the task into smaller chunks, applies the operation (e.g., extract the domain from a URL) and then aggregates the counts.
We have two implementations for this. One, through python functions (e.g., for [domain counts](wimbd/url_counts/)), easily extentable and scalable.
The second implementation is through Rust, for faster processing. The [Rust implementation](wimbd/src/) covers the summary statistics (presented in Table 2 in the paper) such as the corpus size, number of tokens, etc. In addition, it computes the most & least common $n$-grams approximation using bloom filters.

In practice, we implement search using [elasticsearch](https://www.elastic.co/). We index 5 of the corpora we consider, and provide both a UI and a programmatic access to those.
We built some wrappers around the ES API, which allows `count` and `extract` funcitonalities. We provide a more detailed documentation [here](./wimbd/es/README.md).


## Getting started

### Python

#### Create python environment
```
conda create -n wimbd python=3.9
conda activate wimbd

pip install -r requirements.txt

export PYTHONPATH="${PYTHONPATH}:/PATH/TO/wimbd/"
```

As an example, run the following command that counts the domain counts, per token (Section 4.2.2 in the paper):
```sh
bash wimbd/url_per_tok_counts/run.sh /PATH-TO/c4/en/c4-train.* > data/benchmark/benchmark_url_tok_c4.jsonl
```


#### Run scheme counts

```
./wimbd/scheme_counts/run.sh /PATH-TO/laion2B-en/*.gz > data/scheme_laion2B-en.jsonl
```

This will run the map reduce scripts, and dump the results into a file



### Rust


This part of the repository is written in Rust, so first you'll have to [install the Rust toolchain](https://www.rust-lang.org/tools/install). There's a simple one-liner for that:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then compile the release binary:

```bash
make release DIR=./bin
```

And now you can run it:

```bash
./bin/wimbd --help
```

For example, find the top 20 3-grams in some c4 files with:

```bash
./bin/wimbd topk \
    /PATH-TO/c4/en/c4-train.01009-of-01024.json.gz \
    /PATH-TO/c4/en/c4-train.01010-of-01024.json.gz \
    -n 3 \
    -k 20 \
    --size 16GiB
```

## Search

Due to the nature of ElasticSearch, we cannot release the API keys on the web.
If you are interested in using our ElasticSearch indices, please fill up this [form](https://forms.gle/Mk9uwJibR9H4hh9Y9), and we'll get back to you as soon as we can.


## Issues
If there's an issue with the code, or you have questions, feel free to [open an issue](https://github.com/allenai/wimbd/issues/new/choose)
or send a [PR](https://github.com/allenai/wimbd/compare)

