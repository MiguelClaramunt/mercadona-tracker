import datetime
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from mercatracker.globals import load_dotenv, update_variable
from utils import dicts, dt

config = load_dotenv(".env.shared")


def process_response(response: requests.Response) -> str:
    item = response.json()
    item = dicts.flatten_dict(item)
    return item


def get_soup(url: str = config["URL_SITEMAP"]) -> BeautifulSoup:
    xml = requests.get(url).text

    return BeautifulSoup(xml, features="xml")


def get_tag(soup, tag: str) -> str:
    value = soup.find(tag).get_text("", True)

    return value


def get_tags(soup, tag: str, skiprows: int = 1) -> list[str]:
    all_found = soup.find_all(tag)[skiprows:]

    return [found.get_text("", True) for found in all_found]


def get_ids(soup) -> list[str]:
    return search_all(get_tags(soup, tag="loc", skiprows=1), pattern=r"(\d+(?:\.\d+)?)")


def search_all(strings: list[str], pattern: str = r"(\d+(?:\.\d+)?)") -> list[str]:
    return [search(string=string, pattern=pattern) for string in strings]


def search(string, pattern) -> str:
    search = re.search(pattern, string)
    if search:
        return search.group(1)


def get_lastmod(soup) -> int:
    date = get_tag(soup, "lastmod")
    lastmod = dt.iso_date2custom_format(date, custom_format=config["FORMAT_DATE"])
    update_variable(
        variable="LAST_MOD_DATE", value=lastmod, dotenv_file=config["DOTENV_SHARED"]
    )

    return int(lastmod)


def get_current_ids(filename: str, dtypes: dict = {"id": str}) -> set[str]:
    try:
        ids_checked = set(
            pd.read_csv(filename, usecols=dtypes.keys(), dtype=dtypes)[dtypes.keys()]
            .iloc[:, 0]
            .values.tolist()
        )
    except FileNotFoundError:
        ids_checked = set()

    return ids_checked
