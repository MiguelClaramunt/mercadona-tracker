import re
from dataclasses import dataclass, field
from functools import partial
from operator import is_not
from typing import List

import requests
from bs4 import BeautifulSoup

from mercatracker import temporal


@dataclass
class Soup:
    url: str
    soup: BeautifulSoup = field(init=False)

    def request(self) -> "Soup":
        xml = requests.get(self.url).text
        self.soup = BeautifulSoup(xml, features="xml")
        return self

    def get_tag(self, tag: str) -> str:
        return self.soup.find(tag).get_text("", True)

    def get_tags(self, tag: str, skiprows: int = 1) -> list[str]:
        found = self.soup.find_all(tag)[skiprows:]

        return [found.get_text("", True) for found in found]

    def get_ids(self) -> List[str]:
        strings = self.get_tags(tag="loc", skiprows=1)

        return self.search(strings, pattern=r"(\d+(?:\.\d+)?)")

    @classmethod
    def search(cls, strings: list[str], pattern: str = r"(\d+(?:\.\d+)?)") -> list[str]:
        found_items = [
            found.group(1)
            for string in strings
            if (found := re.search(pattern, string))
        ]
        return list(filter(partial(is_not, None), found_items))

    # @classmethod
    # def search(cls, string, pattern) -> str:
    #     search = re.search(pattern, string)
    #     if search:
    #         return search.group(1)

    def get_lastmod(self) -> int:
        date = self.get_tag("lastmod")
        lastmod = temporal.iso_date2custom_format(date)

        return int(lastmod)
