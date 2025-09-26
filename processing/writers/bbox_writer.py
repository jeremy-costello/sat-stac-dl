import asyncio
import pyarrow as pa
from pystac_client import Client
from concurrent.futures import ThreadPoolExecutor
from tqdm.asyncio import tqdm_asyncio
from processing.utils.bbox_utils import get_bbox_from_point
from processing.utils.point_utils import (
  sample_points_per_geometry, generate_random_points_async
)
from processing.utils.census_utils import CanadaHierarchy


POINTS_PER_CSD = 2  # 5161 per
POINTS_PER_CD = 40  # 293 per
POINTS_PER_PR = 1000  # 13 per
POINTS_OVER_CANADA = 25_000  # 1 per


def create_bbox_table(con):
    # Create table
    # should the id be auto incremented?
    con.execute("""
        CREATE TABLE IF NOT EXISTS canada_bboxes (
            id INTEGER PRIMARY KEY,
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
            tile_size INTEGER
        )
        """)


async def insert_points_async(con):
    loop = asyncio.get_running_loop()

    # --- 1) Sample points asynchronously ---
    csd_points = await sample_points_per_geometry("./data/inputs/census_subdiv", "CSDUID", n_points_per_geom=POINTS_PER_CSD)
    cd_points = await sample_points_per_geometry("./data/inputs/census_div", "CDUID", n_points_per_geom=40)
    pr_points = await sample_points_per_geometry("./data/inputs/prov_terr", "PRUID", n_points_per_geom=1000)
    rand_points = await generate_random_points_async(25_000)

    all_points = []
    for pts, src in [(csd_points, "CSD"), (cd_points, "CD"), (pr_points, "PR"), (rand_points, "RAND")]:
        print(f"✅ {len(pts)} points sampled from {src}")
        all_points.extend(pts)

    # --- 2) Calculate hierarchies across threads ---
    hierarchy_helper = CanadaHierarchy()
    hierarchies = [None] * len(all_points)

    def calc_hierarchy(idx_pt):
        idx, pt = idx_pt
        return idx, hierarchy_helper.infer_hierarchy(pt)

    with ThreadPoolExecutor() as executor:
        futures = [loop.run_in_executor(executor, calc_hierarchy, (i, pt)) for i, pt in enumerate(all_points)]
        for f in tqdm_asyncio.as_completed(futures, total=len(futures), desc="Calculating hierarchy"):
            idx, hierarchy = await f
            hierarchies[idx] = hierarchy

    # --- 3) Construct a single PyArrow Table ---
    # Prepare lists of column data
    ids = list(range(1, len(all_points) + 1))
    lons = [p["lon"] for p in all_points]
    lats = [p["lat"] for p in all_points]
    pr_names = [h.get("PRNAME") for h in hierarchies]
    pr_uids = [h.get("PRUID") for h in hierarchies]
    cd_names = [h.get("CDNAME") for h in hierarchies]
    cd_uids = [h.get("CDUID") for h in hierarchies]
    csd_names = [h.get("CSDNAME") for h in hierarchies]
    csd_uids = [h.get("CSDUID") for h in hierarchies]

    # Create the PyArrow Table from the column data
    arrow_table = pa.Table.from_pydict({
        "id": ids,
        "lon": lons,
        "lat": lats,
        "province": pr_names,
        "province_id": pr_uids,
        "census_div": cd_names,
        "census_div_id": cd_uids,
        "census_subdiv": csd_names,
        "census_subdiv_id": csd_uids
    })

    # --- 4) Perform single, high-performance insert from PyArrow Table ---
    # Register the PyArrow Table as a view with a simpler name for the SQL command
    # DuckDB will then do a zero-copy insert from the Arrow table
    con.register("arrow_table_view", arrow_table)

    await loop.run_in_executor(
        None,
        lambda: con.execute("""
            INSERT INTO canada_bboxes (
                id, lon, lat,
                province, province_id, census_div, census_div_id,
                census_subdiv, census_subdiv_id
            ) SELECT * FROM arrow_table_view
        """)
    )

    print(f"🎉 Inserted all {len(all_points)} points into the DB.")


async def update_bboxes_async(con, resolution_m, tile_size):
    loop = asyncio.get_running_loop()

    # --- 1) Fetch existing points ---
    rows = await loop.run_in_executor(None, lambda: con.execute(
        "SELECT id, lon, lat FROM canada_bboxes"
    ).fetchall())
    print(f"📦 Retrieved {len(rows)} rows from DB")

    # --- 2) Compute bbox/resolution across threads ---
    def compute_bbox(row):
        row_id, lon, lat = row
        bbox_info = get_bbox_from_point(lon, lat, resolution_m, tile_size)
        return {
            "id": row_id,
            "bbox": bbox_info["bbox"],
            "resolution_deg": bbox_info["deg_resolution"],
            "resolution_m": resolution_m,
            "tile_size": tile_size
        }

    results = []
    with ThreadPoolExecutor() as executor:
        futures = [
            loop.run_in_executor(executor, compute_bbox, row)
            for row in rows
        ]
        for f in tqdm_asyncio.as_completed(futures, total=len(futures), desc="Calculating bboxes"):
            result = await f
            results.append(result)

    # --- 3) Construct a single PyArrow Table ---
    arrow_table = pa.Table.from_pydict({
        "id": [r["id"] for r in results],
        "bbox": [r["bbox"] for r in results],
        "resolution_deg": [r["resolution_deg"] for r in results],
        "resolution_m": [r["resolution_m"] for r in results],
        "tile_size": [r["tile_size"] for r in results],
    })

    # --- 4) Perform single, high-performance update ---
    # Register Arrow table as a view
    con.register("bbox_table_view", arrow_table)

    # Use an UPDATE .. FROM pattern so we don’t do row-by-row updates
    await loop.run_in_executor(
        None,
        lambda: con.execute("""
            UPDATE canada_bboxes
            SET bbox = v.bbox,
                resolution_deg = v.resolution_deg,
                resolution_m = v.resolution_m,
                tile_size = v.tile_size
            FROM bbox_table_view v
            WHERE canada_bboxes.id = v.id
        """)
    )

    print(f"✅ Updated bbox and resolution for {len(rows)} points.")


async def update_rcm_items(con):
    loop = asyncio.get_running_loop()

    # 1) Fetch all rows
    rows = await loop.run_in_executor(
        None, lambda: con.execute("SELECT id, bbox FROM canada_bboxes").fetchall()
    )
    print(f"🛰️ Retrieved {len(rows)} rows for RCM update")

    # Open catalog once
    stac_url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac"
    catalog = Client.open(stac_url)

    # 2) Define fetch function
    def fetch_rcm_sync(bbox):
        search = catalog.search(
            collections=["rcm-ard"],
            bbox=bbox,
            datetime="2019-06-12/2048-01-01",
            limit=1,
            method="GET"
        )
        return list(search.items())

    async def fetch_rcm(row_id, bbox):
        items = await asyncio.to_thread(fetch_rcm_sync, bbox)
        return {"id": row_id, "rcm_items": [item.id for item in items] if items else []}

    # 3) Fetch items concurrently
    tasks = [fetch_rcm(row_id, bbox) for row_id, bbox in rows]
    results = []
    for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Fetching RCM"):
        result = await f
        results.append(result)

    # 4) Build PyArrow Table
    arrow_table = pa.Table.from_pydict({
        "id": [r["id"] for r in results],
        "rcm_items": [r["rcm_items"] for r in results],
    })

    # 5) Register + bulk update
    con.register("rcm_view", arrow_table)
    await loop.run_in_executor(
        None,
        lambda: con.execute("""
            UPDATE canada_bboxes
            SET rcm_items = v.rcm_items
            FROM rcm_view v
            WHERE canada_bboxes.id = v.id
        """)
    )
    print(f"✅ Updated RCM items for {len(results)} rows.")
