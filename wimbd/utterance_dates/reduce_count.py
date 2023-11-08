import json
import sys
from collections import defaultdict


def main():
    data = defaultdict(int)

    for line in sys.stdin:
        data[line.strip()] += 1

    for k, v in data.items():
        print(json.dumps({'year': k, 'count': v}))


if __name__ == "__main__":
    main()
