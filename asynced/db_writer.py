import os
import asyncio
import duckdb
from asynced.writers_main import (
  create_main_table, insert_points_async, update_bboxes_async
)
from asynced.writers_land import (
    create_landcover_table, update_landcover_from_tiff
)


MAIN_DB_PATH = "./data/outputs/rcm_ard_tiles.duckdb"
LANDCOVER_DB_PATH = "./data/outputs/landcover_stats.duckdb"
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
    con = duckdb.connect(MAIN_DB_PATH)
    # create_main_table(con)
    # await insert_points_async(con)
    # await update_bboxes_async(con, resolution_m, tile_size)

    landcon = duckdb.connect(LANDCOVER_DB_PATH)
    create_landcover_table(landcon)
    await update_landcover_from_tiff(con, landcon)
    landcon.close()

    # await update_rcm_items(con)

    con.close()
    print("🎉 Finished pipeline and stored all data in DuckDB.")


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
