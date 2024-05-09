import re
import sqlite3
import sys

import pandas as pd
import streamlit as st
from streamlit.web.cli import main
import plotly.express as px

pd.options.plotting.backend = "plotly"


def main():
    conn = sqlite3.connect("mercadona.db")

    # query = """
    # SELECT *
    # FROM prices
    # where [price_instructions.unit_name] is NULL
    # """

    query = """
    SELECT avg([price_instructions.reference_price]) as [price_instructions.reference_price], l1, l1_name
    FROM prices LEFT JOIN ids_categories
    USING(id)
    where [price_instructions.unit_name] is NULL
    group by l1
    """

    # query = """
    # SELECT id, l1, l1_name, prices.ymd
    # FROM prices LEFT JOIN ids_categories
    # USING(id)
    # WHERE l1 is NULL;
    # """

    df = pd.read_sql(
        query,
        conn,
        # dtype={"id": str, "ymd": int, "price_instructions.reference_price": float},
    )
    fig = px.histogram(df, x="l1_name", y='price_instructions.reference_price', color='l1_name')
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig)
    st.table(df)
    # st.line_chart(df, x="ymd", y="price_instructions.reference_price", color="id")

    # fig = px.line(df, x="ymd", y="price_instructions.reference_price", color="id")
    # st.plotly_chart(fig)




if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])
    sys.exit(main())
