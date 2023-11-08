import json
import gzip
from tqdm import tqdm
import smart_open

def read_jsonl_smart_open_file(in_file: str):
    smart_open
    with smart_open.open(in_file, "rt", encoding="UTF8") as f:
        for line in f:
            yield json.loads(line)

def read_json_gz_file(in_file: str):
    with gzip.open(in_file, "rt", encoding="UTF8") as f:
        for line in f:
            yield json.loads(line)


def read_jsonl_file(in_file: str):
    with open(in_file, 'r') as f:
        for line in f:
            yield json.loads(line.strip())


def read_domains(in_f, key):
    with open(in_f, 'r') as f:
        data = f.readlines()
    domains = {}

    for row in tqdm(data):
        try:
            j = json.loads(row.strip().replace('\'', '"'))
            domains[j[key]] = j['count']
        except:
            print(row)
    return dict(sorted(domains.items(), key=lambda item: item[1], reverse=True))
