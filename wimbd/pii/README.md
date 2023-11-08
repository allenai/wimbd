# PII Detection

This folder contains the code for computing the amount of personal identifiable information (PII) in datasets. PII is “information which can be used to distinguish or trace an individual’s identity, such as their
name, social security number, biometric records, etc.”

We document three kinds of personal identifiable information in pretraining corpora: phone numbers, email addresses, and
IP addresses. We use regular expressions to identify instances of these three PII types. The code in this repository is used to compute the number of matches for these three PII types. 


We ran the code as follows:

```
./run.sh <PATH_TO_DATASET>/*json.gz > <OUTPUT_FILE>
```
