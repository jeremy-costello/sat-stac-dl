import duckdb
import pandas as pd


TABLE_NAME = "rcm_ard_items"


pd.set_option('display.max_columns', None)

con = duckdb.connect(f"./data/outputs/rcm_ard_tiles.duckdb")

# Fetch all rows into pandas
df = con.execute(f"SELECT * FROM {TABLE_NAME}").df()
df = con.execute(f"""
    SELECT *
    FROM {TABLE_NAME}
    WHERE array_length(items) > 0
""").df()

print(df.head())
print(len(df))
