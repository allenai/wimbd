import json
import sys
from collections import defaultdict


def main():

    data = defaultdict(int)

    for line in sys.stdin:
        language_count, count = line.strip().rsplit(maxsplit=1)
        data[language_count] += int(count)

    for k, v in data.items():
        print(json.dumps({'language': k, 'count': v}))


if __name__ == "__main__":
    main()
