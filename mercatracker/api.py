import random
import time
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

from mercatracker.globals import load_dotenv

config = load_dotenv(".env.shared")


def build_url(id: str, params: dict = config["PARAMS_API"]) -> str:
    """Generates an URL based on a product ID

    Args:
        id (str): Product ID
        params (dict, optional): API parameters. Defaults to config["PARAMS_API"] = {'lang': 'es', 'wh': 'vlc1'}.

    Returns:
        str: URL
    """
    url = f"{config['URL_API']}/{id}/"
    url_parts = list(urlparse(url))
    query = dict(parse_qsl(url_parts[4]))
    query.update(params)
    url_parts[4] = urlencode(query)

    return urlunparse(url_parts)


def call(id: str) -> requests.Response:
    url = build_url(id)
    time.sleep(random.uniform(0, 1))
    return requests.get(url)
