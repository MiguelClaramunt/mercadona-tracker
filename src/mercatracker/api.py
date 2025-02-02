import datetime
import json
import re
from abc import ABC
from dataclasses import dataclass, field
from functools import partial
from operator import is_not
from typing import List, Optional, Self

import requests
from bs4 import BeautifulSoup
from certifi import where as certifi_where
from yarl import URL


from mercatracker.config import Config

config = Config().load()


class TemporaryRestrictionException(Exception):
    pass


def iso_date2custom_format(
    iso_date: str | datetime.date, custom_format: str = r"%Y%m%d"
) -> str:
    if isinstance(iso_date, datetime.date):
        iso_date = str(iso_date)
    date = datetime.date.fromisoformat(iso_date)
    return date.strftime(custom_format)


@dataclass
class AbstractRequestHandler(ABC):
    url: str = field(init=False)
    headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
        }
    )
    response: requests.Response = field(init=False)
    request_sitemap: Optional[bool] = None
    scheme: str = "https"

    def request(self) -> requests.Response:
        try:
            self.response = requests.get(
                self.url,
                headers=self.headers,
                verify=certifi_where(),
            )
        except requests.exceptions.SSLError as e:
            raise e
        return self.response

    def request_soup(self) -> Self:
        xml = requests.get(self.url).text
        self.soup = BeautifulSoup(xml, features="xml")
        return self

    def get_tags(self, tag: str, skiprows: int = 1) -> list[str]:
        found = self.soup.find_all(tag)[skiprows:]

        return [found.get_text("", True) for found in found]

    def scrape_ids(self) -> List[str]:
        self.request_soup()
        strings = self.get_tags(tag="loc", skiprows=1)
        ids = self.search_pattern(strings, pattern=r"(\d+(?:\.\d+)?)")

        return [id for id in ids if id]

    @classmethod
    def search_pattern(
        cls, strings: list[str], pattern: str = r"(\d+(?:\.\d+)?)"
    ) -> list[str]:
        found_items = [
            found.group(1)
            for string in strings
            if (found := re.search(pattern, string))
        ]
        return list(filter(partial(is_not, None), found_items))

    def to_dump(self) -> str:
        """Return the JSON content of the response as a string."""
        data = self.response.json()
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)

    def get_tag(self, tag: str) -> str:
        return self.soup.find(tag).get_text("", True)

    def scrape_lastmod(self) -> int:
        date = self.get_tag("lastmod")
        self.lastmod = iso_date2custom_format(date)

        return int(self.lastmod)


@dataclass(kw_only=True)
class MercadonaProductSchema(AbstractRequestHandler):
    id: Optional[str] = None
    params: dict[str, str] = field(default_factory=lambda: {"lang": "es", "wh": "vlc1"})
    host: str = "tienda.mercadona.es"

    def __post_init__(self):
        if (self.id is None) and (self.request_sitemap is None):
            raise ValueError("Either 'id' or 'request_sitemap' must be provided.")
        if self.request_sitemap:
            self.url = str(
                URL.build(
                    scheme=self.scheme,
                    host=self.host,
                    path="/sitemap.xml",
                )
            )
            self.__call__()
        else:
            self.url = str(
                URL.build(
                    scheme=self.scheme,
                    host=self.host,
                    path=f"/api/v1_1/products/{self.id}/",
                    query=self.params,
                )
            )

    def __call__(self):
        soup = self.request_soup()
        self.lastmod = soup.scrape_lastmod()

    def request_soup(self) -> Self:
        super().request_soup()
        if "800 500 220" in self.soup.get_text():
            raise TemporaryRestrictionException("Service misuse detected.")
        return self


@dataclass(kw_only=True)
class ConsumProductSchema(AbstractRequestHandler):
    id: str
    params: dict[str, str] = field(default_factory=lambda: {"shippingZoneId": "0D"})
    host: str = "tienda.consum.es"

    def __post_init__(self):
        if (self.id is None) and (self.request_sitemap is None):
            raise ValueError("Either 'id' or 'request_sitemap' must be provided.")
        if self.request_sitemap:
            self.url = str(
                URL.build(
                    scheme=self.scheme,
                    host=self.host,
                    path="/sitemap_product_es.xml",
                )
            )
            # get lastmod from current date
            self.lastmod = iso_date2custom_format(date.today())
        else:
            self.url = str(
                URL.build(
                    scheme=self.scheme,
                    host=self.host,
                    path=f"/api/rest/V1.0/catalog/product/code/{self.id}",
                    query=self.params,
                )
            )

    def __call__(self):
        self.lastmod = int(datetime.date.today().strftime("%Y%m%d"))
