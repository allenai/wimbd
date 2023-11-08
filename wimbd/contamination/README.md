# Contamination Analysis

This folder contains the code for extracting the list of datasets that will be analyzed in the data contamination analysis.

The intuition was to look for a large benchmark, where the different inputs and outputs for the task can be easily extractable, and we can do it programatically, for scaling purposes.
We ended up finding [PromptSource](https://github.com/bigscience-workshop/promptsource) to be a good fit, as those properties are extractable from the prompts.


We ran the code in this directory as follows:

```sh
python wimbd/contamination/promptsource_parse.py --path /PATH-TO/promptsource/promptsource/templates --out_file resources/p3_datasets.tsv
```