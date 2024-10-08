import asyncio
import base64
import itertools
import json
import time
from dataclasses import dataclass, field
from typing import Self
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import aiohttp
import requests
from requests.adapters import HTTPAdapter, Retry
from yarl import URL

from mercatracker import processing
from mercatracker.config import Config

config = Config().load()


@dataclass
class ProductSchema(asyncio.Protocol):
    id: str
    params: dict
    url: str = field(init=False)
    headers = {"Content-Type": "application/json", "X-Api-Key": config.x_api_key}

    def __post_init__(self):
        # self.url = self.build_url()
        self.url = str(
            URL.build(
                scheme="https",
                host="tienda.mercadona.es",
                path=f"/api/v1_1/products/{self.id}/",
                query=self.params,
            )
        )

    @classmethod
    def build_url(cls) -> str:
        url = f"{config['URL_API']}/{cls.id}/"
        url_parts = list(urlparse(url))
        query = dict(parse_qsl(url_parts[4]))
        query.update(cls.params)
        url_parts[4] = urlencode(query)

        return urlunparse(url_parts)

    def request_force_retry(self) -> requests.Response:
        retry_count = itertools.count()
        while True:
            try:
                counter = next(retry_count)
                # get with timeout and convert http errors to exceptions
                self.response = requests.get(
                    self.url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
                    },
                    verify="/home/miguel/miniconda3/envs/merca/lib/python3.12/site-packages/certifi/cacert.pem",
                    # verify=False,
                )
                if self.response.status_code == 200:
                    # time.sleep(0.1)
                    pass
                elif self.response.status_code == 410:
                    return self.response
                elif self.response.status_code == 429 and counter <= 10:
                    time.sleep(counter * 15)
                    continue
                elif self.response.status_code == 504:
                    continue
                self.response.raise_for_status()
            # the things you want to recover from
            except requests.Timeout or requests.HTTPError as e:
                if next(retry_count) <= 10:
                    print("timeout, wait and retry:", e)
                    time.sleep(retry_count * 30)
                    continue
                else:
                    print("timeout, exiting")
                    raise  # reraise exception to exit
            except requests.exceptions.SSLError:
                raise
            except Exception as e:
                print("unrecoverable error", e)
                raise
            break

        return self.response

    # def request(self) -> Self:

    def post(self) -> Self:
        data = {"url": self.url, "httpResponseBody": True}

        self.response = requests.post(
            config.proxyscrape_api_url, headers=self.headers, json=data
        )
        return self.response

    def decode(self) -> Self:
        # self.decoded = json.loads(
        #     base64.b64decode(self.response.json()["data"]["httpResponseBody"]).decode()
        # )
        self.decoded = self.response.json()
        return self.decoded

    # @Semaphore(2)
    # async def async_post(
    #     self,
    # ) -> requests.Response:
    #     data = {"url": self.url, "httpResponseBody": True}
    #     response = await requests.post(
    #         config.proxyscrape_api_url, headers=self.headers, json=data
    #     )
    #     return response

    # @Semaphore(2)
    # async def run_post(self) -> requests.Response:
    #     try:
    #         # Using await to handle the response asynchronously
    #         response = await self.async_post()

    #         return response
    #     except Exception as e:
    #         print(f"Error sending request: {e}")

    async def async_post(self, session: aiohttp.ClientSession):
        data = {"url": self.url, "httpResponseBody": True}
        async with session.post(
            config.proxyscrape_api_url, headers=self.headers, json=data
        ) as response:
            self.response = response.text()

    def async_decode(self):
        self.decoded = json.loads(
            base64.b64decode(
                (json.loads(self.response))["data"]["httpResponseBody"]
            ).decode()
        )
        return self.decoded


@dataclass
class ProductRequest(requests.Response):
    # @retry(requests.exceptions.Timeout, tries=-1, delay=30, jitter=(0, 30))
    def request(self, url) -> Self:
        session = requests.Session()
        retries = Retry(
            total=None, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504]
        )

        session.mount("http://", HTTPAdapter(max_retries=retries))

        response = session.get(
            url,
            headers=config.headers,
            verify=config.verify,
        )

        self.status = response.status_code
        self.response = response.json()

        return self

    def process(self) -> dict:
        return processing.flatten_dict(self.response)


@dataclass
class ProxyScrapeProductRequest(requests.Response):
    headers = {"Content-Type": "application/json", "X-Api-Key": config.x_api_key}

    def post(
        self,
        url,
    ) -> Self:
        data = {"url": url, "httpResponseBody": True}

        self.response = requests.post(
            config.proxyscrape_api_url, headers=self.headers, json=data
        )
        return self.response

    def decode(
        self,
    ) -> Self:
        self.decoded = json.loads(
            base64.b64decode(self.response.json()["data"]["httpResponseBody"]).decode()
        )
        return self.decoded
