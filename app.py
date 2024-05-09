import streamlit as st
import pandas as pd
import sqlite3

conn = sqlite3.connect('mercadona.db')

query = """
SELECT display_name, [price_instructions.unit_price], ymd
FROM products
"""

df = pd.read_sql(query, conn)
st.table(df)
