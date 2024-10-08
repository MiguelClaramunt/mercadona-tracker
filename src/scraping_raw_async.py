import asyncio
import json

# import libsql_experimental as libsql
import sqlite3

import pandas as pd
from tqdm import tqdm
import aiohttp

from mercatracker import api, db, logging
from mercatracker.config import Config
from mercatracker.scraper import Soup

pd.set_option("future.no_silent_downcasting", True)

async def main():
    config = Config().load()
    conn = sqlite3.connect("/home/miguel/git/mercadona-tracker/src/mercadona_test.db")

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

    products = [api.ProductSchema(id=id, params=config.params_api) for id in ids]

    async with aiohttp.ClientSession() as session:
        tasks = [product.fetch(session) for product in products]
    
    htmls = await asyncio.gather(*tasks)

    with tqdm(total=len(ids)) as pbar:
        for product, result in zip(products, results):
            if result.status_code == 200:
                db.write_dump(
                    conn,
                    (
                        product.id,
                        str(product.decode()),
                        lastmod,
                    ),
                )
                pbar.update(1)

if __name__ == "__main__":
    asyncio.run(main())
