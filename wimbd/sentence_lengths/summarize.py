import json
import os
import tqdm

lengths_data_path = "/home/alanes/wimbd/lengths_data/"

for filename in os.listdir(lengths_data_path):
    toks_data = dict()
    chars_data = dict()
    dataset = filename.split(".")[0]

    print(f'analyzing {dataset}')

    with open(os.path.join(lengths_data_path, filename)) as infile:
        for line in tqdm.tqdm(infile.readlines()):
            data = json.loads(line)
            key = data['key']
            count = int(data['count'])

            if key == 'text is None':
                continue

            count_type, value = key.split(' ')
            length = int(value)
            if count_type.lower() == 'tokens':
                chars_data[length] = count
            elif count_type.lower() == 'chars':
                toks_data[length] = count

    count_sum = sum(toks_data.values())
    dist = {length: count / count_sum for length, count in toks_data.items()}
    print(f'Found {count_sum} total token datapoints; values sum to {sum(dist.values())}')
    with open(os.path.join("/home/alanes/wimbd/lengths_tok_summary/", f'toks_{dataset}.json'), 'w') as ofile:
        json.dump(dist, ofile)

    count_sum = sum(chars_data.values())
    dist = {length: count / count_sum for length, count in chars_data.items()}
    print(f'Found {count_sum} total char datapoints; values sum to {sum(dist.values())}')
    with open(os.path.join("/home/alanes/wimbd/lengths_char_summary/", f'chars_{dataset}.json'), 'w') as ofile:
        json.dump(dist, ofile)
