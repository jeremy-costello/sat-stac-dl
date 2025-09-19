import os
import asyncio
import duckdb
from tqdm.asyncio import tqdm_asyncio
from concurrent.futures import ThreadPoolExecutor
from shared.utils import get_bbox_from_point
from asynced.stac_items import get_rcm_items, get_landcover_items
from asynced.writers import create_table, insert_points_async


DB_PATH = "./data/outputs/rcm_ard_tiles.duckdb"
RESOLUTION_M = 30
TILE_SIZE = 256

# -----------------------------
# Config & DB Setup
# -----------------------------
os.makedirs("./data/outputs", exist_ok=True)


# -----------------------------
# Main pipeline
# -----------------------------
async def main_async(resolution_m, tile_size):
    con = duckdb.connect(DB_PATH)

    create_table(con)
    await insert_points_async(con)
    await update_bboxes_async(con, resolution_m, tile_size)
    # await update_landcover_items()
    # await update_rcm_items()

    con.close()
    print("🎉 Finished pipeline and stored all data in DuckDB.")


# -----------------------------
# Async Step 2: Update bboxes
# -----------------------------
async def update_bboxes_async(con, resolution_m, tile_size):
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


# # -----------------------------
# # Steps 3 & 4: Landcover and RCM (unchanged)
# # -----------------------------
# async def update_landcover_items():
#     rows = con.execute("SELECT id, bbox FROM rcm_ard_tiles").fetchall()
#     tasks = [fetch_landcover(row_id, bbox) for row_id, bbox in rows]
#     for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
#         await f

# async def fetch_landcover(row_id, bbox):
#     items = await get_landcover_items(bbox)
#     item_ids = [item.id for item in items] if items else []
#     con.execute("UPDATE rcm_ard_tiles SET landcover_items = ? WHERE id = ?", (item_ids, row_id))

# async def update_rcm_items():
#     rows = con.execute("SELECT id, bbox FROM rcm_ard_tiles").fetchall()
#     tasks = [fetch_rcm(row_id, bbox) for row_id, bbox in rows]
#     for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
#         await f

# async def fetch_rcm(row_id, bbox):
#     items = await get_rcm_items(bbox)
#     item_ids = [item.id for item in items] if items else []
#     con.execute("UPDATE rcm_ard_tiles SET rcm_items = ? WHERE id = ?", (item_ids, row_id))





if __name__ == "__main__":
    asyncio.run(main_async(resolution_m=RESOLUTION_M, tile_size=TILE_SIZE))
