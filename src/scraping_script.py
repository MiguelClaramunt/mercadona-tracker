# %%
import os
import sys
import time

import pandas as pd

from mercatracker import (
    api,
    globals,
    io,
    path,
    processing,
    reporting,
    scraping,
    temporal,
)

pd.set_option("future.no_silent_downcasting", True)

config = globals.load_dotenv()

if not temporal.updated_ids():
    soup = scraping.get_soup(url=config["URL_SITEMAP"])
    config["LASTMOD_DATE"] = scraping.get_lastmod(soup)
    all_ids = scraping.get_ids(soup)
else:
    all_ids = config["ALL_IDS"]

filename = path.build(config["ITEMS_FOLDER"], config["LASTMOD_DATE"], "csv")

checked = scraping.get_current_ids(filename=filename)
ids = (checked.copy(), all_ids)

reporting.soup(config["LASTMOD_DATE"], ids=ids)

start = time.monotonic()
try:
    for id in all_ids:
        if id not in checked and id:
            product = api.Product(id=id, params=config["PARAMS_API"])
            response = product.request()
            if response.status_code == 200:
                item = product.process(response)
                df = processing.items2df(
                    item,
                    lastmod_date=config["LASTMOD_DATE"],
                    columns=config["_ITEMS_COLUMNS"],
                )
                io.df2csv(
                    df,
                    filename,
                    columns=config["ITEMS_COLUMNS"],
                )
                checked.add(id)
            if response.status_code == 410:
                all_ids.pop(all_ids.index(id))
                globals.update_variable(
                    variables={"ALL_IDS": str(all_ids)},
                    file=config["DOTENV_TEMP"],
                )

except KeyboardInterrupt:
    print("Exiting script...")
    reporting.performance(
        times=(start, time.monotonic()),
        ids=(
            checked,
            scraping.get_current_ids(filename=filename),
        ),
    )
    try:
        sys.exit(130)
    except SystemExit:
        os._exit(130)
else:
    reporting.performance(times=(start, time.monotonic()), ids=ids)
