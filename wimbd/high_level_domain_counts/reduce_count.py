import json
import sys
from collections import defaultdict


def main():

    data = defaultdict(int)

    for line in sys.stdin:
        high_level_domain, count = line.strip().split()
        data[high_level_domain] += int(count)

    
    for k, v in data.items():
        print(json.dumps({'high_level_domain': k, 'count': v}))
	

if __name__ == "__main__":
    main()

