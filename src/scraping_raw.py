import json
import os

# import libsql_experimental as libsql
import sqlite3
import sys

import pandas as pd
from tqdm import tqdm

from mercatracker import api, db, logging
from mercatracker.config import Config
from mercatracker.scraper import Soup

pd.set_option("future.no_silent_downcasting", True)

config = Config().load()

conn = sqlite3.connect("/home/miguel/git/mercadona-tracker/src/mercadona.db")
cur = conn.cursor()

soup = Soup(url=config.url_sitemap).request()
lastmod = soup.get_lastmod()

if lastmod != db.get_lastmod(conn):
    all_ids = soup.get_ids()
    db.write_scraped_ids(
        conn,
        (
            lastmod,
            json.dumps(all_ids),
        ),
    )
    # config["LASTMOD"] = lastmod
    # config.update(file="temp", var={"LASTMOD": lastmod})
else:
    all_ids = db.get_scraped_ids(conn, lastmod)


ids = set(all_ids) - set(db.get_processed_ids(conn, lastmod))
logging.soup(lastmod)


with tqdm(total=len(ids)) as pbar:
    try:
        for id in ids:
            product = api.ProductSchema(id=id, params=config.params_api)
            # request = api.ProxyScrapeProductRequest()
            # response = request.post(product.url)
            response = product.request_force_retry()

            if response.status_code == 200:
                db.write_dump(
                    conn,
                    (
                        id,
                        # str(request.decode()),
                        str(product.decode()),
                        lastmod,
                    ),
                )
                pbar.update(1)
                # ids.remove(id)

            if response.status_code == 410:
                continue
                # print(all_ids.index(id))
                # ids.remove(id)
                # config.update(file="temp", var={"ALL_IDS": all_ids})

    except KeyboardInterrupt:
        conn.close()
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
