import os
import re
from collections import Counter
from itertools import product
from urllib.parse import urlparse

from dotenv import dotenv_values

from .dicts import flatten_dict_recursive
import pandas as pd
# from .api import retrieve_last_mod_date

config = {
    **dotenv_values(".env.shared"),  # load shared development variables
}

# LAST_MOD_DATE = retrieve_last_mod_date(config['SITEMAP_URL'])


def transform_suppliers(suppliers_dict: list) -> str:
    suppliers_list = [
        value for dictionary in suppliers_dict for value in dictionary.values()
    ]
    suppliers_str = "\t".join(suppliers_list)

    return suppliers_str if suppliers_str else None


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


def remove_html_tags(string: str, pattern: str) -> str:
    return re.sub(pattern, "", str(string))


def extract_thumbnail_id(url: str) -> str:
    return os.path.splitext(os.path.basename(urlparse(url).path))[0]


def items2df(items: list | dict, LAST_MOD_DATE) -> pd.DataFrame:
    if isinstance(items, dict):
        df = pd.DataFrame([items])
    else:
        df = pd.DataFrame(items)

    df["is_bulk"] = df["is_bulk"].fillna(False)
    df["extra_info"] = df["extra_info"].apply(
        lambda x: next(map(str, x)) if not x == None else None
    )
    df["details.suppliers"] = df["details.suppliers"].apply(transform_suppliers)
    df["l1"], df["l1_name"], df["l2"], df["l2_name"], df["l3"], df["l3_name"] = zip(
        *df["categories"].apply(
            transform_categories, args=("categories", 2, ("id", "name"))
        )
    )
    df = df.drop(
        ["photos", "categories", "unavailable_weekdays", "limit", "share_url"], axis=1
    )
    for col in ["nutrition_information.allergens", "nutrition_information.ingredients"]:
        df[col] = df[col].apply(remove_html_tags, args=(r"<[^<]+>",))
    df["thumbnail"] = df["thumbnail"].apply(extract_thumbnail_id)
    df["ymd"] = LAST_MOD_DATE

    df = df.astype(
        {
            "ean": "int64",
            "price_instructions.iva": "int8",
            "price_instructions.reference_price": "float32",
            "price_instructions.unit_price": "float32",
            "price_instructions.unit_size": "float32",
            "price_instructions.bulk_price": "float32",
            "price_instructions.size_format": "category",
            "price_instructions.unit_name": "category",
        }
    )
    df = df.replace(r"^\s*$", None, regex=True)

    return df.convert_dtypes()


def df2csv(item: pd.DataFrame, LAST_MOD_DATE) -> None:
    filename = f"./items/{LAST_MOD_DATE}.csv"

    if not os.path.isfile(filename):
        item.to_csv(filename, header="column_names", index=False)
    else:  # else it exists so append without writing the header
        item.to_csv(filename, mode="a", header=False, index=False)
