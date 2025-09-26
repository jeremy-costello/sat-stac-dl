import os
import asyncio
import duckdb
from processing.writers.bbox_writer import (
  create_bbox_table, insert_points_async, update_bboxes_async
)
from processing.writers.census_writer import update_census_data
from processing.writers.landcover_writer import (
    create_landcover_table, update_landcover_from_tiff
)
from processing.writers.rcm_writer import (
    create_rcm_ard_tables, update_rcm_ard_tables
)
from processing.wriers.tile_writer import (
  create_rcm_ard_tiles_table, download_rcm_tiles
)


DB_PATH = "./data/outputs/rcm_ard.duckdb"
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
    # create_bbox_table(con)
    # await insert_points_async(con)
    # await update_bboxes_async(con, resolution_m, tile_size)

    # create_landcover_table(con)
    # await update_landcover_from_tiff(con)
    # await update_census_data(con)

    # await create_rcm_ard_tables(con)
    # await update_rcm_ard_tables(con)

    await create_rcm_ard_tiles_table(con)
    await download_rcm_tiles(con)

    con.close()
    print("🎉 Finished pipeline and stored all data in DuckDB.")


if __name__ == "__main__":
    asyncio.run(main_async(resolution_m=RESOLUTION_M, tile_size=TILE_SIZE))
