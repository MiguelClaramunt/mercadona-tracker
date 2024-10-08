# %%
import os
import sys
import time

import pandas as pd
from tqdm import tqdm

from mercatracker import api, io, logging, processing
from mercatracker.config import Config
from mercatracker.scraper import Soup

pd.set_option("future.no_silent_downcasting", True)

config = Config().load()

soup = Soup(url=config.url_sitemap).request()
LASTMOD_DATE = soup.get_lastmod()

if not LASTMOD_DATE == str(config.lastmod):
    config["LASTMOD"] = LASTMOD_DATE
    all_ids = soup.get_ids()
    config.update(file="temp", var={"LASTMOD": LASTMOD_DATE})
else:
    all_ids = config.all_ids

# file = io.File(config[["ROOT_PATH", "ITEMS_FOLDER", "LASTMOD"]], ".csv")
# file.write_header(
#     pd.DataFrame(columns=config.items_cols).to_csv(
#         header=True, index=False, columns=config.items_cols
#     )
# )

conn = libsql.connect("mercadona_2.db")
cur = conn.cursor()


checked = file.read(dtypes={"id": str})

ids = set(all_ids) - set(checked)
logging.soup(config.lastmod)

with tqdm(total=len(ids)) as pbar:
    try:
        for id in ids:
            if id and id not in checked:
                product = api.ProductSchema(id=id, params=config.params_api)
                time.sleep(0.8)
                request = api.ProductRequest(product.url).request()
                if product.status == 200:
                    product_processed = product.process()
                    df = processing.items2df(
                        product_processed,
                        lastmod_date=config.lastmod,
                        columns=config._items_cols,
                    )
                    io.df2csv(
                        df,
                        file.path,
                        columns=config.items_cols,
                    )
                    checked.append(id)
                    pbar.update(1)
                if product.status == 410:
                    print(all_ids.index(id))
                    all_ids.pop(all_ids.index(id))
                    config.update(file="temp", var={"ALL_IDS": all_ids})

    except KeyboardInterrupt:
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
