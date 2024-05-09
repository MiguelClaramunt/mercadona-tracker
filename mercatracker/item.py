import ast
import datetime
import os
import re
from collections import Counter
from dataclasses import dataclass, field
from itertools import product
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from backoff import expo, on_exception
from dotenv import dotenv_values
from ratelimit import RateLimitException, limits

import utils

config = {
    **dotenv_values(".env.shared")
}

@dataclass
class Item:
    id: str
    _last_mod_date: int
    _url: str = ''
    _is_checked: bool = False
    _params: dict = ast.literal_eval(config['API_PARAMS'])

    def __post_init__(self):
        object.__setattr__(self, '_url', self.build_url())


    def build_url(self) -> str:
        url = f"{config['API_BASE_URL']}/{self.id}/"
        url_parts = list(urlparse(url))
        query = dict(parse_qsl(url_parts[4]))
        query.update(self._params)
        url_parts[4] = urlencode(query)
        
        return urlunparse(url_parts)
    

    def response2items(response: requests.Response) -> str:
        item = response.json()
        item = utils.dicts.flatten_dict(item)
        
        return item
    

    @on_exception(expo, RateLimitException, max_tries=30, base=15, factor=2, max_time=200)
    @limits(calls=3, period=1)
    def call(self):
        response = requests.get(self._url)

        if response.status_code == 429:
            raise Exception('API response: {}'.format(response.status_code))
        if response.status_code == 200:
            return response


 