import os
import re
from collections import Counter
from collections.abc import MutableMapping
from itertools import product
from urllib.parse import urlparse

import pandas as pd

from mercatracker.config import Config

config = Config().load()


def remove_html_tags(string: str, pattern: str) -> str:
    return re.sub(pattern, "", str(string))


def batch_split(elements: list, batch_size: int) -> list[set[str]]:
    return [
        set(elements[i : i + batch_size]) for i in range(0, len(elements), batch_size)
    ]


def flatten_dict(
    d: MutableMapping, parent_key: str = "", sep: str = "_"
) -> MutableMapping:
    items = []
    for k, v in d.items():
        new_key = sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)


def flatten_dict_recursive(d):
    out = {}
    for key, val in d.items():
        if isinstance(val, dict):
            val = [val]
        if isinstance(val, list):
            for subdict in val:
                deeper = flatten_dict_recursive(subdict).items()
                out.update({key + "." + key2: val2 for key2, val2 in deeper})
        else:
            out[key] = val

    return out


def transform_suppliers(suppliers_list: list) -> str:
    return "\t".join([i["name"] for i in suppliers_list])


def transform_categories(
    categories_list: list,
    key_recursive: str,
    recursivity_levels: int,
    key_retrieve: tuple[str,],
) -> tuple[str]:
    if recursivity_levels < 0:
        raise ValueError("'recursivity_levels' cannot be less than zero")

    if type(key_retrieve) != tuple:
        raise TypeError("'key_retrieve' must be a tuple")

    categories_dict = flatten_dict_recursive(categories_list[0])

    if recursivity_levels > max(Counter(key)["."] for key in categories_dict.keys()):
        raise ValueError(
            f"parameter 'recursivity_levels' ({recursivity_levels}) greater than maximum level ({max(Counter(key)['.'] for key in categories_dict.keys())})"
        )
    else:
        keys_product = product(
            (f"{key_recursive}." * i for i in range(recursivity_levels + 1)),
            key_retrieve,
        )
        keys = (prefix + suffix for prefix, suffix in keys_product)

    return [categories_dict[key] for key in keys]


def extract_thumbnail_id(url: str) -> str:
    return os.path.splitext(os.path.basename(urlparse(url).path))[0]


# def items2df(
#     items: list | dict,
#     columns: list,
#     lastmod_date: int = -1,
# ) -> pd.DataFrame:

#     if isinstance(items, dict):
#         df = pd.DataFrame([items], columns=columns)
#     else:
#         df = pd.DataFrame(items, columns=columns)

#     if isinstance(lastmod_date, str):
#         lastmod_date = int(lastmod_date)
#         df["ymd"] = lastmod_date
#     elif lastmod_date < 0 and columns == config["ITEMS_COLUMNS"]:
#         lastmod_date = config['LASTMOD']
#         df["ymd"] = lastmod_date
#     else:
#         lastmod_date = df["ymd"]

#     # df["extra_info"] = df["extra_info"].apply(
#     #     lambda x: next(map(str, x)) if not x == None else None
#     # )
#     if "details.suppliers" in df.columns and isinstance(df["details.suppliers"], str):
#         df["details.suppliers"] = df["details.suppliers"].apply(transform_suppliers)

#     if "categories" in df.columns:
#         df["l1"], df["l1_name"], df["l2"], df["l2_name"], df["l3"], df["l3_name"] = zip(
#             *df["categories"].apply(
#                 transform_categories, args=("categories", 2, ("id", "name"))
#             )
#         )
#     if "is_bulk" in df.columns:
#         df["is_bulk"] = df["is_bulk"].fillna(False)
#     else:
#         df["is_bulk"] = False

#     df = df.drop(
#         [
#             "photos",
#             "categories",
#             "unavailable_weekdays",
#             "limit",
#             "share_url",
#             "unavailable_from",
#             "extra_info",
#             # "details.suppliers",
#             # "details.counter_info",
#             # "details.danger_mentions",
#             # "details.alcohol_by_volume",
#             # "details.mandatory_mentions",
#             # "details.production_variant",
#             # "details.usage_instructions",
#             # "details.storage_instructions",
#             # "nutrition_information.allergens",
#         ],
#         axis=1,
#         errors="ignore",
#     )

#     for col in ["nutrition_information.allergens", "nutrition_information.ingredients"]:
#         df[col] = df[col].str.replace(
#             r"<[^<]+>", ""
#         )  # .apply(remove_html_tags, args=(r"<[^<]+>",))
#     df["thumbnail"] = df["thumbnail"].str.extract(r"([\w|\d]{32})")

#     # df = df.astype(config["ITEMS_DTYPES"])
#     df = df.replace(r"^\s*$", None, regex=True)

#     return df.convert_dtypes()


def generate_dataframe(items: list | dict) -> pd.DataFrame:
    if isinstance(items, dict):
        items = [items]

    return pd.DataFrame(items)


def items2df(
    items: list | dict, lastmod_date: int = -1, columns: list = config["ITEMS_COLS"]
) -> pd.DataFrame:
    df = (
        generate_dataframe(items)
        .rename(columns={"_brand": "brand1", "_origin": "origin1"})
        .rename(columns=lambda x: re.sub("^_", "", x))
    )

    if isinstance(lastmod_date, str):
        lastmod_date = int(lastmod_date)
    elif lastmod_date < 0:
        lastmod_date = config["LASTMOD"]

    # df["extra_info"] = df["extra_info"].apply(
    #     lambda x: next(map(str, x)) if not x == None else None
    # )
    if "suppliers" in df.columns and not isinstance(df["suppliers"][0], str):
        df["suppliers"] = df["suppliers"].apply(transform_suppliers)

    if "categories" in df.columns:
        df["l1"], df["l1_name"], df["l2"], df["l2_name"], df["l3"], df["l3_name"] = zip(
            *df["categories"].apply(
                transform_categories, args=("categories", 2, ("id", "name"))
            )
        )
    if "is_bulk" in df.columns:
        df["is_bulk"] = df["is_bulk"].fillna(False)
    else:
        df["is_bulk"] = False

    df = df.drop(
        [
            "photos",
            "categories",
            "unavailable_weekdays",
            "limit",
            "share_url",
            "unavailable_from",
            "extra_info",
        ],
        axis=1,
        errors="ignore",
    )

    for col in ["allergens", "ingredients"]:
        df[col] = df[col].apply(remove_html_tags, args=(r"<[^<]+>",))
    df["thumbnail"] = df["thumbnail"].apply(extract_thumbnail_id)
    df["ymd"] = lastmod_date

    # df = df.astype(config["ITEMS_DTYPES"])
    df = df.replace(r"^\s*$", None, regex=True)

    return df.convert_dtypes()


# def process(
#     response: requests.Response, lastmod: int, columns=list[str]
# ) -> pd.DataFrame:
#     item = scraping.process_response(response)
#     return items2df(item, lastmod_date=lastmod, columns=columns)
