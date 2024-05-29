# %%
import os
import sys
import time

import pandas as pd
from tqdm import tqdm

from mercatracker import api, io, processing, reporting
from mercatracker.config import Config
from mercatracker.scraper import Soup

pd.set_option("future.no_silent_downcasting", True)

config = Config().load()

soup = Soup(url=config["URL_SITEMAP"]).request()
LASTMOD_DATE = soup.get_lastmod()

if not LASTMOD_DATE == str(config["LASTMOD_DATE"]):
    config["LASTMOD_DATE"] = LASTMOD_DATE
    all_ids = soup.get_ids()
    config.update(file="temp", var={"LASTMOD_DATE": LASTMOD_DATE})
else:
    all_ids = config["ALL_IDS"]

file = io.File(config[["ROOT_PATH", "ITEMS_FOLDER", "LASTMOD_DATE"]], ".csv")
file.write_header(
    pd.DataFrame(columns=config["ITEMS_COLS"]).to_csv(
        header=True, index=False, columns=config["ITEMS_COLS"]
    )
)

checked = file.read(dtypes={"id": str})

ids = set(all_ids) - set(checked)
reporting.soup(config["LASTMOD_DATE"])

with tqdm(total=len(ids)) as pbar:
    try:
        for id in ids:
            if id and id not in checked:
                product = api.Product(id=id, params=config["PARAMS_API"])
                time.sleep(0.5)
                response = product.request_force_retry()
                if response.status_code == 200:
                    item = product.process(response)
                    df = processing.items2df(
                        item,
                        lastmod_date=config["LASTMOD_DATE"],
                        columns=config["_ITEMS_COLS"],
                    )
                    io.df2csv(
                        df,
                        file.path,
                        columns=config["ITEMS_COLS"],
                    )
                    checked.append(id)
                    pbar.update(1)
                if response.status_code == 410:
                    all_ids.pop(all_ids.index(id))
                    config.update(file="temp", var={"ALL_IDS": all_ids})

    except KeyboardInterrupt:
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)