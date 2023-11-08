import argparse

import requests
from wimbd.utils.utils import read_json_gz_file


def check_url_exists(url: str):
    """
    Checks if a url exists
    :param url: url to check
    :return: True if the url exists, false otherwise.
    """
    # return requests.head(url, allow_redirects=True).status_code == 200
    try:
        with requests.get(url, stream=True) as response:
            try:
                response.raise_for_status()
                return True
            except requests.exceptions.HTTPError:
                return False
    except requests.exceptions.ConnectionError:
        return False


def main():

    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str)

    args = parse.parse_args()

    data = read_json_gz_file(args.in_file)

    for row in data:
        print(check_url_exists(row['url']))


if __name__ == "__main__":
    main()
