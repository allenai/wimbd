import argparse
import multiprocessing as mp
from wimbd.utils.utils import read_jsonl_smart_open_file
import hashlib
import os
import gzip
import json
from tqdm import tqdm
import numpy as np
import smart_open
from collections import defaultdict

def process_shard(shard, args):
    # read shard
    data = read_jsonl_smart_open_file(shard)

    duplicates_removed = 0
    duplicates_per_source = defaultdict(int)

    def write_doc(doc, fout, contaminated, just_write_attributes):
        if just_write_attributes:
            fout.write(json.dumps({'id':doc['id'], 'source':doc['source'], 'contaminated':contaminated}) + '\n')
        else:
            if contaminated:
                pass
            else:
                fout.write(json.dumps(doc) + '\n')

    

    with smart_open. open(os.path.join(args.outpath, os.path.basename(shard)), 'wt') as fout:
        for doc in data:
            # hash incoming doc
            if doc['text'] is None:
                print('None')
                continue
            h = hashlib.md5()
            h.update(doc['text'].encode())
            hash = h.hexdigest()

            # check blocklist
            if hash in blocklist:
                if args.keep_first_occurrence == False:
                    # skip doc
                    duplicates_removed += 1
                    duplicates_per_source[doc['source'] if 'source' in doc else 'no_source'] += 1
                    write_doc(doc, fout, contaminated=True, just_write_attributes=args.just_write_attributes)
                    continue

                lock.acquire()
                try:
                    if seen_array[blocklist[hash]] == True:
                        # skip doc
                        duplicates_removed += 1
                        duplicates_per_source[doc['source'] if 'source' in doc else 'no_source'] += 1
                        write_doc(doc, fout, contaminated=True, just_write_attributes=args.just_write_attributes)
                        continue
                    else:
                        # update the blocklist
                        seen_array[blocklist[hash]] = True
                finally:
                    lock.release()
            
            # write it out
            write_doc(doc, fout, contaminated=False, just_write_attributes=args.just_write_attributes)

    return duplicates_removed, duplicates_per_source



def worker_initializer(l, block, seen):
    global blocklist
    global lock
    global seen_array

    lock = l
    blocklist = block
    seen_array = seen


def main(args):
    # Create global objects
    
    blocklist = dict()
    lock = mp.Lock()

    # Add the hashes to the blocklist
    with open(args.blocklist, 'rt') as fin:
        for i, hash in tqdm(enumerate(fin), desc="Loading blocklist"):
            blocklist[hash.strip()] = i
    
    seen_array = np.memmap(args.blocklist+".seen.dat", dtype=bool, mode='w+', shape=len(blocklist))

    # Create a pool of processes
    with mp.Pool(processes=args.num_processes, initializer=worker_initializer, initargs=(lock, blocklist, seen_array)) as pool:
        # Process each shard
        results = [pool.apply_async(process_shard, args=(shard, args)) for shard in args.shards]
        removed_counts = [result.get() for result in tqdm(results, desc="Processing shards")]

        # Wait for all the processes to finish
        pool.close()
        pool.join()

    total_removed_by_source = defaultdict(int)
    total_removed = 0
    for shard, (removed_count, removed_by_source) in zip(args.shards, removed_counts):
        print(f"{removed_count} documents matched in {shard}")
        total_removed += removed_count
        for source, count in removed_by_source.items():
            total_removed_by_source[source] += count
    for source, count in total_removed_by_source.items():
        print(f"{count} documents were matched from {source}")
    print(f"A total of {total_removed} documents were matched")

if __name__ == "__main__":
    parse = argparse.ArgumentParser("")

    parse.add_argument("--shards", nargs='+', type=str)
    parse.add_argument("--blocklist", type=str)
    parse.add_argument("--outpath", type=str)
    parse.add_argument("--keep_first_occurrence", action="store_true")
    parse.add_argument("--num_processes", type=int, default=mp.cpu_count())
    # an arg just_write_attributes that just writes an attribute file instead of the full doc
    parse.add_argument("--just_write_attributes", action="store_true")


    args = parse.parse_args()

    if args.num_processes > len(args.shards):
        args.num_processes = len(args.shards)

    main(args)
