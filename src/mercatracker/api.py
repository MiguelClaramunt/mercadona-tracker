import itertools
import time
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
from retry import retry

from mercatracker import processing
from mercatracker.config import Config

config = Config().load()


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

    def request_force_retry(self) -> requests.Response:
        retry_count = itertools.count()
        while True:
            try:
                counter = next(retry_count)
                # get with timeout and convert http errors to exceptions
                resp = requests.get(
                    self._url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.6496.3 Safari/537.36",
                    },
                )
                if resp.status_code == 410:
                    return resp
                if resp.status_code == 429 and counter <= 10:
                    time.sleep(counter * 15)
                    continue
                if resp.status_code == 504:
                    continue
                resp.raise_for_status()
            # the things you want to recover from
            except requests.Timeout or requests.HTTPError:
                # if next(retry_count) <= 10:
                #     print("timeout, wait and retry:", e)
                #     time.sleep(retry_count * 30)
                #     continue
                # else:
                print("timeout, exiting")
                raise  # reraise exception to exit
            except Exception as e:
                print("unrecoverable error", e)
                raise
            break

        return resp
