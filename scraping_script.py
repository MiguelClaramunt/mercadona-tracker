# %%
import os
import sys
import time

import pandas as pd

from mercatracker import etl, globals, path, reporting, scraping, temporal

pd.set_option("future.no_silent_downcasting", True)

config = globals.load_dotenv()

filename = path.build(config["ITEMS_FOLDER"], config["LASTMOD_DATE"], "csv")

if not temporal.updated_ids():
    soup = scraping.get_soup(url=config["URL_SITEMAP"])
    config["LASTMOD_DATE"] = scraping.get_lastmod(soup)
    all_ids = scraping.get_ids(soup)
else:
    all_ids = config["ALL_IDS"]

ids_checked = scraping.get_current_ids(filename=filename)
ids = (ids_checked.copy(), all_ids)

reporting.soup(config["LASTMOD_DATE"], ids=ids)

start = time.monotonic()
try:
    etl.scrape_items(ids, filename=filename)
except KeyboardInterrupt:
    print("Exiting script...")
    reporting.performance(
        times=(start, time.monotonic()),
        ids=(
            ids_checked,
            scraping.get_current_ids(filename=filename),
        ),
    )
    try:
        sys.exit(130)
    except SystemExit:
        os._exit(130)
else:
    reporting.performance(times=(start, time.monotonic()), ids=ids)
