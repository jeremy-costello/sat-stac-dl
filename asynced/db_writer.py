import os
import json
import asyncio
import duckdb
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from concurrent.futures import ThreadPoolExecutor
from shared.utils import get_bbox_from_point, sample_points_per_csd
from asynced.stac_items import get_rcm_items, get_landcover_items

# -----------------------------
# Config & DB Setup
# -----------------------------
os.makedirs("./data/outputs", exist_ok=True)
DB_PATH = "./data/outputs/metadata.duckdb"
con = duckdb.connect(DB_PATH)

# Load mappings
with open("./data/inputs/PR_mapping.json") as f:
    prid_to_prname = json.load(f)
with open("./data/inputs/CD_mapping.json") as f:
    cdid_to_cdname = json.load(f)
with open("./data/inputs/CSD_mapping.json") as f:
    csdid_to_csdname = json.load(f)

# Create table
con.execute("""
CREATE TABLE IF NOT EXISTS rcm_ard_tiles (
    id INTEGER,
    lon DOUBLE,
    lat DOUBLE,
    province TEXT,
    province_id INTEGER,
    census_div TEXT,
    census_div_id INTEGER,
    census_subdiv TEXT,
    census_subdiv_id INTEGER,
    bbox DOUBLE[],
    resolution_deg DOUBLE[],
    resolution_m DOUBLE,
    tile_size INTEGER,
    rcm_items TEXT[],
    landcover_items TEXT[],
    rcm_file TEXT,
    landcover_file TEXT
)
""")

# -----------------------------
# Async Step 1: Insert points
# -----------------------------
async def insert_points_async(n_points_per_subdiv=1):
    points = await asyncio.to_thread(sample_points_per_csd,
                                     cs_shp="./data/inputs/census_subdiv",
                                     n_points_per_subdiv=n_points_per_subdiv)

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        tasks = []
        for i, pt in enumerate(points, start=1):
            csd_id = pt["CSDID"]
            province_id = int(str(csd_id)[:2])
            census_div_id = int(str(csd_id)[:4])
            province = prid_to_prname.get(str(province_id), "Unknown")
            census_div = cdid_to_cdname.get(str(census_div_id), "Unknown")
            census_subdiv = csdid_to_csdname.get(str(csd_id), "Unknown")

            # Schedule insertion in a thread
            tasks.append(loop.run_in_executor(executor, con.execute, """
                INSERT INTO rcm_ard_tiles (
                    id, lon, lat,
                    province, province_id, census_div, census_div_id,
                    census_subdiv, census_subdiv_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (i, pt["lon"], pt["lat"],
                  province, province_id, census_div, census_div_id,
                  census_subdiv, csd_id)))
        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
            await f
    print(f"✅ Inserted {len(points)} points into the DB.")


# -----------------------------
# Async Step 2: Update bboxes
# -----------------------------
async def update_bboxes_async(resolution_m=30, tile_size=256):
    rows = con.execute("SELECT id, lon, lat FROM rcm_ard_tiles").fetchall()
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        tasks = []
        for row in rows:
            row_id, lon, lat = row
            bbox_info = get_bbox_from_point(lon, lat, resolution_m, tile_size)
            tasks.append(loop.run_in_executor(executor, con.execute, """
                UPDATE rcm_ard_tiles SET
                    bbox = ?, resolution_deg = ?, resolution_m = ?, tile_size = ?
                WHERE id = ?
            """, (list(bbox_info["bbox"]),
                  list(bbox_info["deg_resolution"]),
                  resolution_m, tile_size, row_id)))
        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
            await f
    print(f"✅ Updated bbox and resolution for {len(rows)} points.")


# -----------------------------
# Steps 3 & 4: Landcover and RCM (unchanged)
# -----------------------------
async def update_landcover_items():
    rows = con.execute("SELECT id, bbox FROM rcm_ard_tiles").fetchall()
    tasks = [fetch_landcover(row_id, bbox) for row_id, bbox in rows]
    for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
        await f

async def fetch_landcover(row_id, bbox):
    items = await get_landcover_items(bbox)
    item_ids = [item.id for item in items] if items else []
    con.execute("UPDATE rcm_ard_tiles SET landcover_items = ? WHERE id = ?", (item_ids, row_id))

async def update_rcm_items():
    rows = con.execute("SELECT id, bbox FROM rcm_ard_tiles").fetchall()
    tasks = [fetch_rcm(row_id, bbox) for row_id, bbox in rows]
    for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
        await f

async def fetch_rcm(row_id, bbox):
    items = await get_rcm_items(bbox)
    item_ids = [item.id for item in items] if items else []
    con.execute("UPDATE rcm_ard_tiles SET rcm_items = ? WHERE id = ?", (item_ids, row_id))


# -----------------------------
# Main pipeline
# -----------------------------
async def main_async(n_points_per_subdiv=1, resolution_m=30, tile_size=256):
    await insert_points_async(n_points_per_subdiv)
    await update_bboxes_async(resolution_m, tile_size)
    await update_landcover_items()
    await update_rcm_items()
    con.close()
    print("🎉 Finished pipeline and stored all data in DuckDB.")


if __name__ == "__main__":
    asyncio.run(main_async(n_points_per_subdiv=1, resolution_m=30, tile_size=256))
