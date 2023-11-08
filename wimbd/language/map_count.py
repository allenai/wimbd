import argparse
from collections import defaultdict
import pycld2 as cld2

from wimbd.utils.utils import read_json_gz_file

def model_classification(batched_inputs: list[str]):
    predictions = []
    for text_input in batched_inputs:
        try:
            is_reliable, _, details = cld2.detect(text_input)
            pred = details[0][1] if is_reliable else 'un'
            predictions.append(pred)
        except cld2.error:
            ...
    return predictions


def main():
    parser = argparse.ArgumentParser("")
    parser.add_argument("--in_file", type=str)
    parser.add_argument("--bs", type=int, default=1000)

    args = parser.parse_args()
    data = read_json_gz_file(args.in_file)
    bs = args.bs
    inputs = []

    predict = model_classification

    val_dic = defaultdict(int)
    for row in data:
        if len(inputs) == bs:
            preds = predict(inputs)
            for p in preds:
                val_dic[p] += 1
            inputs = []
        if not row['text'] or row['text'].strip() == '':
            continue
        if len(row['text']) > 99999:
            val_dic['long'] += 1
            continue
        inputs.append(row['text'])
    if len(inputs) > 0:
        preds = predict(inputs)
        for p in preds:
            val_dic[p] += 1
    for k, v in val_dic.items():
        print(k, v)


if __name__ == "__main__":
    main()
