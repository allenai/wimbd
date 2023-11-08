import argparse
from wimbd.utils.utils import read_json_gz_file
import hashlib
from tqdm.autonotebook import tqdm
import json


def find_hash_example(data_shards, hashes_to_find, k = 30, with_repeats = False):
    found_hashes = {}
    if len(hashes_to_find) == 0:
        return found_hashes

    examples_found = 0
    for filename in tqdm(data_shards):
        data = read_json_gz_file(filename)
        for row in data:
            if examples_found >= k:
                return found_hashes
            if row['text'] is None:
                hash = 'None'
                row['text'] = 'None'
            else:
                h = hashlib.md5()
                h.update(row['text'].encode())
                hash = h.hexdigest()
            
            if hash in hashes_to_find:
                found_hashes[hash] = row['text']
                if not with_repeats:
                    hashes_to_find.remove(hash)
                examples_found +=1
    
    return found_hashes

def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--data_shards", type=str, nargs='+')
    parse.add_argument("--hash_file", type=str)
    parse.add_argument("--count_hash_file", type=str)
    parse.add_argument("--count_url_file", type=str)
    parse.add_argument("--k", type=int, default=30)
    parse.add_argument("--outfile", type=str)
    parse.add_argument("--with_repeats", action='store_true')


    args = parse.parse_args()
    # only one of hash_file or count_hash_file or count_url_file should be provided
    assert sum([args.hash_file is not None, args.count_hash_file is not None, args.count_url_file is not None]) == 1, "Must provide one of --hash_file or --count_hash_file or --count_url_file"
    
    if args.count_url_file is not None:
        with open(args.count_url_file) as fin:
            output_data = []
            for l in fin:
                count, text = l.strip().split()
                output_data.append({"text":text, "count":count})
        
        # sort by count descending
        output_data = sorted(output_data, key=lambda x: int(x['count']), reverse=True)
        if args.outfile:
            with open(args.outfile, 'w') as fout:
                for line in output_data:
                    fout.write(json.dumps(line) + '\n')
        return

    if args.hash_file is not None:
        with open(args.hash_file) as fin:
            hashes_to_find = set(l.strip() for l in fin)
    elif args.count_hash_file is not None:
        with open(args.count_hash_file ) as fin:
            hash2counts = {}
            for l in fin:
                count, hash = l.strip().split()
                hash2counts[hash] = count
            hashes_to_find = set(hash2counts.keys())


    matched_hashes = find_hash_example(args.data_shards, hashes_to_find, args.k, with_repeats=args.with_repeats)

    if args.count_hash_file:
        output_data = [{"hash":hash, "text":matched_hashes[hash], "count":hash2counts[hash]} for hash in matched_hashes]
        # sort by count descending
        output_data = sorted(output_data, key=lambda x: int(x['count']), reverse=True)
    else:
        output_data = [{"hash":hash, "text":matched_hashes[hash]} for hash in matched_hashes]

    if args.outfile:
        with open(args.outfile, 'w') as fout:
            for line in output_data:
                fout.write(json.dumps(line) + '\n')

if __name__ == "__main__":
    main()
