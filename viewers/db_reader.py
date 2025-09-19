import duckdb
import pandas as pd


pd.set_option('display.max_columns', None)

con = duckdb.connect("./data/outputs/rcm_ard_tiles.duckdb")

# Fetch all rows into pandas
df = con.execute("SELECT * FROM rcm_ard_tiles").df()
print(df.head())
