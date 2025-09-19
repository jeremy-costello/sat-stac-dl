import os
import duckdb
from shapely.geometry import box
from shared.utils import save_with_bandnames
from synced.fetch import fetch_data


# Output folders
os.makedirs("./data/outputs", exist_ok=True)
os.makedirs("./data/outputs/rasters", exist_ok=True)

# DuckDB connection
con = duckdb.connect("./data/outputs/metadata.duckdb")

# Enable spatial extension
# con.execute("INSTALL spatial;")
# con.execute("LOAD spatial;")

# Create table with array types
con.execute("""
CREATE TABLE IF NOT EXISTS raster_metadata (
    id INTEGER,
    bbox DOUBLE[],
    resolution_deg DOUBLE[],
    resolution_m DOUBLE,
    tile_size INTEGER,
    rcm_file TEXT,
    landcover_file TEXT,
    province TEXT[],
    province_id TEXT[],
    census_div TEXT[],
    census_div_id TEXT[],
    census_subdiv TEXT[],
    census_subdiv_id TEXT[]
)
""")

results_collected = 0
max_results = 1000
record_id = 1

while results_collected < max_results:
    data = fetch_data(resolution_m=30, tile_size=256)

    # --- Geometry ---
    bbox = data["bbox"]
    deg_res = data["deg_res"]
    resolution_m = data["resolution_m"]
    tile_size = data["tile_size"]

    # --- RCM raster ---
    rcm_file = None
    if data["rcm"] is not None:
        rcm_tile_squeezed = data["rcm"].squeeze(dim="time")
        rcm_bands = list(rcm_tile_squeezed.band.values)
        rcm_file = f"./data/outputs/rasters/rcm_{record_id}.tif"
        save_with_bandnames(rcm_tile_squeezed, rcm_file, rcm_bands)

    # --- Landcover raster ---
    landcover_file = None
    if data["landcover"] is not None:
        landcover_bands = list(data["landcover"].band.values)
        landcover_file = f"./data/outputs/rasters/landcover_{record_id}.tif"
        save_with_bandnames(data["landcover"], landcover_file, landcover_bands)

    # --- Metadata (lists) ---
    md = data["metadata"]

    province = [f.get("PRNAME") for f in md.get("PR", []) if "PRNAME" in f]
    province_id = [f.get("PRUID") for f in md.get("PR", []) if "PRUID" in f]

    census_div = [f.get("CDNAME") for f in md.get("CD", []) if "CDNAME" in f]
    census_div_id = [f.get("CDUID") for f in md.get("CD", []) if "CDUID" in f]

    census_subdiv = [f.get("CSDNAME") for f in md.get("CSD", []) if "CSDNAME" in f]
    census_subdiv_id = [f.get("CSDUID") for f in md.get("CSD", []) if "CSDUID" in f]

    # --- Insert into DuckDB ---
    con.execute("""
        INSERT INTO raster_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        record_id,
        list(bbox),
        list(deg_res),
        resolution_m,
        tile_size,
        rcm_file,
        landcover_file,
        province or [],
        province_id or [],
        census_div or [],
        census_div_id or [],
        census_subdiv or [],
        census_subdiv_id or [],
    ))

    print(f"✅ Inserted record {record_id} (RCM: {'yes' if rcm_file else 'no'})")
    record_id += 1
    results_collected += 1

con.close()
print("🎉 Finished collecting 10 results and storing metadata in DuckDB.")
