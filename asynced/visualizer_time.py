import duckdb  # or sqlite3 if your DB is SQLite
import pandas as pd
import matplotlib.pyplot as plt

# --- connect to your database ---
# Change this line to how you connect
con = duckdb.connect("./data/outputs/rcm_ard_tiles.duckdb")

# --- read datetimes ---
df = con.execute("SELECT datetime FROM rcm_ard_properties WHERE datetime IS NOT NULL").df()

if df.empty:
    print("No datetime values found.")
else:
    # Convert to datetime dtype
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])

    # --- plot histogram of acquisitions ---
    plt.figure(figsize=(10,5))
    # bin by month — adjust freq to 'D', 'M', 'Y' as you like
    df.groupby(df['datetime'].dt.to_period('M')).size().plot(kind='bar')

    plt.title("RCM ARD acquisitions over time")
    plt.xlabel("Date")
    plt.ylabel("Number of scenes")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
