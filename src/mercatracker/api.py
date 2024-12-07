from abc import ABC
from dataclasses import dataclass, field

import requests
from yarl import URL

from mercatracker.config import Config

config = Config().load()


@dataclass
class AbstractRequestHandler(ABC):
    url: str = field(init=False)
    headers: dict = field(
        default_factory=lambda: {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
        }
    )
    response: requests.Response = field(init=False)

    def request(self) -> requests.Response:
        """Make an HTTP GET request and store the response."""
        try:
            self.response = requests.get(
                self.url,
                headers=self.headers,
                verify="/home/miguel/miniconda3/envs/merca/lib/python3.12/site-packages/certifi/cacert.pem",
            )
        except requests.exceptions.SSLError as e:
            raise e
        return self.response

    def to_dump(self) -> str:
        """Return the JSON content of the response as a string."""
        return self.response.text


@dataclass
class MercadonaProductSchema(AbstractRequestHandler):
    id: str = field(init=False)
    params: dict = field(default_factory=lambda: {"lang": "es", "wh": "vlc1"})

    def __post_init__(self):
        self.url = str(
            URL.build(
                scheme="https",
                host="tienda.mercadona.es",
                path=f"/api/v1_1/products/{self.id}/",
                query=self.params,
            )
        )
