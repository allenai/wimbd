import json
import sys
from collections import defaultdict


def main():

    data = defaultdict(int)

    for line in sys.stdin:
        profane_type, count = line.strip().rsplit(maxsplit=1)
        data[profane_type] += int(count)

    
    for k, v in data.items():
        print(json.dumps({'profanity': k, 'count': v}))
	

if __name__ == "__main__":
    main()

