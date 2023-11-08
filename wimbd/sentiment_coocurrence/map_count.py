import argparse
import itertools
from collections import defaultdict
import json
from pathlib import Path
from spacy.lang.en import English
from spacytextblob.spacytextblob import SpacyTextBlob

from wimbd.utils.utils import read_json_gz_file

# nlp = spacy.load('en_core_web_sm')
nlp = English()
nlp.add_pipe('sentencizer')
nlp.add_pipe("spacytextblob")
nlp.max_length = 100000

def model_classification(batched_inputs: list[str], demographic_terms: dict[list]):
    predictions = defaultdict(float)
    for doc in nlp.pipe(batched_inputs):
        for sent in doc.sents:
            sentiment = sent._.blob.polarity
            sent_tokens = {token.text for token in sent}
            for term in demographic_terms['male_gender_terms']:
                if term in sent_tokens:
                    predictions['male_sum'] += sentiment
                    predictions['male_count'] += 1
            for term in demographic_terms['female_gender_terms']:
                if term in sent_tokens:
                    predictions['female_sum'] += sentiment
                    predictions['female_count'] += 1
            for term in demographic_terms['religious_terms']:
                if term in sent_tokens or term + 's' in sent_tokens:
                    predictions[f'{term}_sum'] += sentiment
                    predictions[f'{term}_count'] += 1
            for term in demographic_terms['racial_terms']:
                if term in sent.text:
                    base_term = term.split()[0]
                    predictions[f'{base_term}_sum'] += sentiment
                    predictions[f'{base_term}_count'] += 1
    return predictions

def expand_demographics():
    file_path = Path(__file__).parent / 'demographic_terms.json'

    with open(file_path, 'r') as f:
        demographic_terms = json.load(f)

    racial_terms = demographic_terms['base_racial_terms']
    person_terms = demographic_terms['person_terms']

    expanded_terms = [' '.join(x) for x in
                      itertools.product(racial_terms, person_terms)]
    demographic_terms['racial_terms'].extend(expanded_terms)
    return demographic_terms

def main():
    parser = argparse.ArgumentParser("")
    parser.add_argument("--in_file", type=str)
    parser.add_argument("--bs", type=int, default=1000)

    args = parser.parse_args()
    data = read_json_gz_file(args.in_file)
    bs = args.bs
    inputs = []

    predict = model_classification

    demographic_terms = expand_demographics()

    val_dic = defaultdict(float)
    for row in data:
        if len(inputs) == bs:
            preds = predict(inputs, demographic_terms)
            for term, p in preds.items():
                val_dic[term] += p
            inputs = []
        if not row['text'] or row['text'].strip() == '':
            continue
        # filtering out really long documents
        if len(row['text']) > 99999:
            continue
        inputs.append(row['text'])
    if len(inputs) > 0:
        preds = predict(inputs, demographic_terms)
        for term, p in preds.items():
            val_dic[term] += p
    for k, v in val_dic.items():
        print(k, v)

if __name__ == "__main__":
    main()
