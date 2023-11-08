import json
import sys
from collections import defaultdict


def main():

    data = defaultdict(int)

    for line in sys.stdin:
        try:
            url, count = line.strip().split()
            data[url] += int(count)
        except:
            pass

    
    for k, v in data.items():
        print(json.dumps({'url': k, 'count': v}))
	

if __name__ == "__main__":
    main()

