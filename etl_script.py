# %%
import sqlite3

from mercatracker import etl, globals

config = globals.load_dotenv(
    dotenv_shared=".env.shared",
)


conn = sqlite3.connect("mercadona.db")

for parameters in config["ETL_PARAMETERS"]:
    to_write = etl.update_database(
        table=parameters["table"],
        conn=conn,
        columns_to_select=parameters["columns_to_select"],
        columns_to_hash=parameters["columns_to_hash"],
        subset_duplicates=parameters["subset_duplicates"],
    )
    if not to_write.empty:
        to_write.to_sql(
            name=parameters["table"], con=conn, index=False, if_exists="append"
        )
    print(f'Table "{parameters["table"]}": {len(to_write)} rows appended.')
# %%
