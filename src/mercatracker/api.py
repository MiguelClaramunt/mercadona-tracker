from dataclasses import dataclass, field

import requests
from yarl import URL

from mercatracker.config import Config

config = Config().load()


@dataclass
class ProductSchema:
    id: str
    params: dict
    url: str = field(init=False)

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

    def request(self) -> requests.Response:
        try:
            self.response = requests.get(
                self.url,
                headers={
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
                },
                verify="/home/miguel/miniconda3/envs/merca/lib/python3.12/site-packages/certifi/cacert.pem",
            )
        except requests.exceptions.SSLError:
            raise requests.exceptions.SSLError

        return self.response

    def to_dump(self) -> str:
        return str(self.response.json())
