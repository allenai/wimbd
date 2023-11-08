import argparse

from dateutil.parser import parse

from wimbd.utils.utils import read_json_gz_file


def extract_date(row):

    def _extract_year(timestamp):
        year = timestamp.split('-')[0]
        if len(year) == 4:
            return year
        return None

    def _parse_and_extract_year(timestamp):
        date = parse(timestamp)
        return date.year

    date_keys = ['date', 'timestamp', 'created']
    for k in date_keys:
        if k in row:
            date_year = _parse_and_extract_year(row[k])
            return date_year
    return None


def main():
    parser = argparse.ArgumentParser("")
    parser.add_argument("--in_file", type=str)
    args = parser.parse_args()
    data = read_json_gz_file(args.in_file)

    for row in data:
        print(extract_date(row))

if __name__ == "__main__":
    main()
