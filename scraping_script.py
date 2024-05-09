# %%
import random
import time
from os import path

import pandas as pd
import requests
from mercatracker import etl, path

import utils
from mercatracker import globals, scraping, reporting, processing, api

pd.set_option("future.no_silent_downcasting", True)

config = globals.load_dotenv(
    dotenv_shared=".env.shared",
)

soup = scraping.get_soup(url=config["URL_SITEMAP"])
config["LAST_MOD_DATE"] = scraping.get_lastmod(soup)
all_ids = scraping.get_ids(soup)

filename = path.build(config["ITEMS_FOLDER"], config["LAST_MOD_DATE"], "csv")

ids_checked = scraping.get_current_ids(filename=filename, dtypes={"id": str})

ids = (ids_checked, all_ids)

reporting.soup(config["LAST_MOD_DATE"], ids=ids)


start = time.monotonic()
etl.scrape_items(ids, filename=filename)
end = time.monotonic()
reporting.performance(times=(start, end), ids=ids)