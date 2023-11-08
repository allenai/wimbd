import json
import sys
from collections import defaultdict
import pandas as pd


def main():

    data = defaultdict(float)

    for line in sys.stdin:
        term, val = line.strip().rsplit(maxsplit=1)
        data[term] += float(val)

    split_values = defaultdict(dict)
    for k, v in data.items():
        term, val_type = k.split('_')
        split_values[val_type][term] = v

    res_df = pd.DataFrame(split_values)
    res = res_df['sum'] / res_df['count']

    print(json.dumps(res.to_dict()))


if __name__ == "__main__":
    main()
