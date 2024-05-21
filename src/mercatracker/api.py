import time
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
from retry import retry

from mercatracker import globals, processing

config = globals.load_dotenv()


@dataclass
class Product:
    id: str
    params: dict
    _url: str = field(init=False)

    def __post_init__(self):
        self._url = self.build_url()

    def build_url(self) -> str:
        url = f"{config['URL_API']}/{self.id}/"
        url_parts = list(urlparse(url))
        query = dict(parse_qsl(url_parts[4]))
        query.update(self.params)
        url_parts[4] = urlencode(query)

        return urlunparse(url_parts)

    @retry(requests.exceptions.Timeout, tries=-1, delay=30, jitter=(0, 30))
    def request(self) -> requests.Response:
        return requests.get(self._url)

    def process(self, response: requests.Response) -> str:
        item = response.json()
        return processing.flatten_dict(item)
