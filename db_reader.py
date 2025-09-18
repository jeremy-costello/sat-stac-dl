import duckdb
import pandas as pd


pd.set_option('display.max_columns', None)

con = duckdb.connect("outputs/metadata.duckdb")

# Fetch all rows into pandas
df = con.execute("SELECT * FROM raster_metadata").df()
print(df.head())
