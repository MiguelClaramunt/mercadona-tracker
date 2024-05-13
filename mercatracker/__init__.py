# from .item import Item
from .etl import update_database, update_dataframe
from .globals import (concatenate_config_columns, generate_etl_parameters,
                      load_dotenv, refresh_variables, update_variable)
from .processing import (extract_thumbnail_id, transform_categories,
                         transform_suppliers)
from .reporting import performance, soup
from .scraping import get_ids, get_soup

# if __name__ == '__main__':
#     main()
