# from .item import Item
from .processing import transform_categories, transform_suppliers, extract_thumbnail_id
from .scraping import get_soup, get_ids
from .globals import (
    load_dotenv,
    update_variable,
    refresh_variables,
    concatenate_config_columns,
)
from .reporting import performance, soup
from .etl import update_database, update_dataframe

# if __name__ == '__main__':
#     main()
