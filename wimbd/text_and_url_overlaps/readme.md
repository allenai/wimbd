# Finding Overlaps in text and urls

## Setup
Make sure to set up your python environment as described in `wimbd/README.md`.

Also the following environment variable will be used in code examples in this readme:

```
export TEXT_AND_URL_OVERLAPS=/path/to/wimbd/wimbd/text_and_url_overlaps
```

## How to use as a decontamination tool

choose files to generate your blocklist from. These can be on s3 or local:

```
in_files1=($(aws s3 ls s3://my_bucket/eval_data/ | awk '{print "s3://my_bucket/eval_data/"$4}'))
```
or
```
in_files1=($(ls /path/to/files/eval_data/*'))
```

Then choose what files to remove contamination from:
```
in_files2=($(aws s3 ls s3://my_bucket/train_data/ | awk '{print "s3://my_bucket/train_data/"$4}'))
```
or
```
in_files2=($(ls /path/to/files/train_data/*'))
```

Finally build the blocklist and remove contamination:
```
python $TEXT_AND_URL_OVERLAPS/decontaminate.py --in_files1 "${in_files1[@]}" --in_files2 "${in_files2[@]}"  --out_dir s3://my_bucket/decontaminated_train_data/ --tmp_dir blocklists/
```

If you've already generated the blocklist you can use that instead of regenerating it
```
python $TEXT_AND_URL_OVERLAPS/decontaminate.py --in_files2 "${in_files2[@]}"  --out_dir s3://my_bucket/decontaminated_train_data/ --blocklist blocklists/blocklist.txt
```


## How to use as a deduplication tool
Similarly to decontamination we can also find text hashes that occur more than once and remove all but the first occurance of each text as follows:

```
python $TEXT_AND_URL_OVERLAPS/deduplicate.py --in_files "${in_files[@]}"  --out_dir s3://my_bucket/deduplicated_train_data/ --tmp_dir blocklists/
```


## How to use for text analysis

You can also use the tools in this directory to generate the data needed for the analysis in `wimbd/notebooks/visualize_text_and_url_overlaps.ipynb`.

First you'll need a json file `text_datasets.json` like this:
```
{
    "<dataset1_name>":"/path/to/dataset1/shards/*.jsonl.gz",
    "<dataset2_name>":"/path/to/dataset2/shards/*.jsonl.gz",
}
```

Then run this command
```
bash $TEXT_AND_URL_OVERLAPS/run_analysis.sh text_datasets.json /path/to/output/dir text
```

Then for any datasets that have url metadata make a `url_datasets.json` with the same format as before and run:
```
bash $TEXT_AND_URL_OVERLAPS/run_analysis.sh url_datasets.json /path/to/output/dir url
```

You can now run `wimbd/notebooks/visualize_text_and_url_overlaps.ipynb` and provide the `/path/to/output/dir` when prompted.