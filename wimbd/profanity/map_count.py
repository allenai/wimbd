import argparse
# https://github.com/dimitrismistriotis/alt-profanity-check
from profanity_check import predict as predict_profanity
import pandas as pd
from wimbd.utils.utils import read_json_gz_file
import re
from collections import defaultdict
from tqdm import tqdm
from spacy.lang.en import English 


profanity_dicts = {
    1: 'profane',
    0: 'not profane'
}
# pattern = None
harmless_re, offensive_min_re, offensive_not_min_re = None, None, None
nlp = English()
nlp.add_pipe('sentencizer')
nlp.max_length = 100000


def split_in_sentences(text):
    doc = nlp(text)
    return [str(sent).strip() for sent in doc.sents]


def model_classification(batched_inputs: list[str], taxonomy_dic: dict, per_sentence: bool = False):
    if per_sentence:
        predictions = []
        for text_input in batched_inputs:
            sentences = split_in_sentences(text_input)
            doc_preds = predict_profanity(sentences)
            predictions.append(int(any(doc_preds)))
    else:
        predictions = predict_profanity(batched_inputs)
    labeled_predictions = [taxonomy_dic[p] for p in predictions]
    return labeled_predictions


def taxonomy_classification(batched_inputs: list[str], taxonomy_dic: dict, per_sentence: bool = False):
    preds = []
    
    for text_input in batched_inputs:
        # search for the pattern in the string
        hamless_matches = harmless_re.findall(text_input)
        offensive_min_matches = offensive_min_re.findall(text_input)
        offensive_not_min_matches = offensive_not_min_re.findall(text_input)
        # loop through the matches and print corresponding values from the dictionary
        # if len(matches) == 0:
        #     print('not profane')
        # else:
        #     for match in matches:

        if len(offensive_min_matches) == 0 and len(offensive_not_min_matches) == 0:
            preds.append('not profane')
        else:
            preds.append('profane')
        
        for _ in offensive_min_matches:
            preds.append('offensive-minority-reference')
        for _ in offensive_not_min_matches:
            preds.append('offensive-not-minority')
        for _ in hamless_matches:
            preds.append('harmless-minority')
        
    return preds


def load_taxonomy():
    # df = pd.read_csv('resources/toxicity.csv').fillna(0)
    df = pd.read_csv('resources/word_based_bias_list.csv').fillna(0)
    # df['body'] = df['body'].astype(int)
    # df['minorityWord'] = df['minorityWord'].astype(int)

    harmless_min = df[df['categorization'] == 'harmless-minority'].word.tolist()
    offensive_min = df[df['categorization'] == 'offensive-minority-reference'].word.tolist()
    offensive_not_min = df[df['categorization'] == 'offensive-not-minority'].word.tolist()
    global harmless_re, offensive_min_re, offensive_not_min_re
    harmless_re = re.compile(r"\b"+r"\b|\b".join(harmless_min)+"\b",re.IGNORECASE)
    offensive_min_re = re.compile(r"\b"+r"\b|\b".join(offensive_min)+"\b",re.IGNORECASE)
    offensive_not_min_re = re.compile(r"\b"+r"\b|\b".join(offensive_not_min)+"\b",re.IGNORECASE)
    
    # df = df.replace({'body': {0: 'not-body', 1: 'body'},
    #             'minorityWord': {0: 'not-minority-word', 1: 'minority-word'},
    #             'badWord': {0: 'not-bad-word', 1: 'bad-word'}})
    # dic = df.set_index('word').T.to_dict('list')
    # dic = {k: '_'.join([str(x) for x in v]) for k, v in dic.items()}
    # return dic


def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str)
    parse.add_argument("--bs", type=int, default=1000)
    parse.add_argument("--model", type=str, choices=['model', 'taxonomy'], default='model')
    parse.add_argument('--per_sentence', action=argparse.BooleanOptionalAction, default=False)

    args = parse.parse_args()

    data = read_json_gz_file(args.in_file)
    bs = args.bs
    inputs = []

    if args.model == 'model':
        predict = model_classification
        taxonomy_dic = profanity_dicts
    else:
        predict = taxonomy_classification
        taxonomy_dic = {}
        # taxonomy_dic = load_taxonomy()
        load_taxonomy()
        # global pattern
        # pattern = re.compile(r"\b(" + "|".join(taxonomy_dic.keys()) + r")\b")

    val_dic = defaultdict(int)
    for ind, row in enumerate(data):
        if len(inputs) == bs:
            preds = predict(inputs, taxonomy_dic, args.per_sentence)
            for p in preds:
                val_dic[p] += 1
            inputs = []
        if not row['text'] or row['text'].strip() == '': continue
        # filtering out really long documents
        if len(row['text']) > 99999:
            val_dic['long'] += 1
            continue
        inputs.append(row['text'])
        if ind % 50000 == 0:
            global nlp
            nlp = English()
            nlp.add_pipe('sentencizer')
            nlp.max_length = 100000
    if len(inputs) > 0:
        preds = predict(inputs, taxonomy_dic, args.per_sentence)
        for p in preds:
            val_dic[p] += 1
    
    for k, v in val_dic.items():
        print(k, v)


if __name__ == "__main__":
    main()
