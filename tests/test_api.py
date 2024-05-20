import requests
from mercatracker import api, globals

config = globals.load_dotenv()


def test_build_url():
    product = api.Product(id="10000", params={"lang": "es", "wh": "vlc1"})
    assert (
        product._url
        == "https://tienda.mercadona.es/api/v1_1/products/10000/?lang=es&wh=vlc1"
    )


def test_request():
    product = api.Product(id="10000", params=config["PARAMS_API"])
    assert type(product.request()) == requests.Response


def test_process():
    product = api.Product(id="10000", params=config["PARAMS_API"])
    response = product.request()

    assert list(product.process(response).keys()) == [
        "id",
        "ean",
        "slug",
        "brand",
        "limit",
        "_is_water",
        "_requires_age_check",
        "origin",
        "photos",
        "_brand",
        "_origin",
        "_suppliers",
        "_legal_name",
        "_description",
        "_counter_info",
        "_danger_mentions",
        "_alcohol_by_volume",
        "_mandatory_mentions",
        "_production_variant",
        "_usage_instructions",
        "_storage_instructions",
        "is_bulk",
        "packaging",
        "published",
        "share_url",
        "thumbnail",
        "categories",
        "extra_info",
        "display_name",
        "unavailable_from",
        "is_variable_weight",
        "_iva",
        "_is_new",
        "_is_pack",
        "_pack_size",
        "_unit_name",
        "_unit_size",
        "_bulk_price",
        "_unit_price",
        "_approx_size",
        "_size_format",
        "_total_units",
        "_unit_selector",
        "_bunch_selector",
        "_drained_weight",
        "_selling_method",
        "_price_decreased",
        "_reference_price",
        "_min_bunch_amount",
        "_reference_format",
        "_previous_unit_price",
        "_increment_bunch_amount",
        "unavailable_weekdays",
        "_allergens",
        "_ingredients",
    ]
