import asyncio
from concurrent.futures import ThreadPoolExecutor
from tqdm.asyncio import tqdm_asyncio
from asynced.utils import sample_points_per_geometry, generate_random_points_async, CanadaHierarchy


def create_table(con):
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

async def insert_points_async(con):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        # --- 1) Sample points per geometry asynchronously ---
        csd_points = await sample_points_per_geometry(
            "./data/inputs/census_subdiv", "CSDUID", n_points_per_geom=5
        )
        cd_points = await sample_points_per_geometry(
            "./data/inputs/census_div", "CDUID", n_points_per_geom=100
        )
        pr_points = await sample_points_per_geometry(
            "./data/inputs/prov_terr", "PRUID", n_points_per_geom=5000
        )

        # --- 2) Sample 250k random points across Canada ---
        rand_points = await generate_random_points_async(250000)

        # --- 3) Combine all points and show counts ---
        all_points = []
        for pts, src in [(csd_points, "CSD"), (cd_points, "CD"), (pr_points, "PR"), (rand_points, "RAND")]:
            print(f"✅ {len(pts)} points sampled from {src}")
            all_points.extend(pts)

        # --- 4) Insert points asynchronously into DuckDB ---
        hierarchy_helper = CanadaHierarchy()

        tasks = []
        for i, pt in enumerate(all_points, start=1):
            hierarchy = hierarchy_helper.infer_hierarchy(pt)
            tasks.append(
                loop.run_in_executor(
                    executor,
                    con.execute,
                    """
                    INSERT INTO rcm_ard_tiles (
                        id, lon, lat,
                        province, province_id, census_div, census_div_id,
                        census_subdiv, census_subdiv_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        i,
                        pt["lon"],
                        pt["lat"],
                        hierarchy["PRNAME"],
                        hierarchy["PRUID"],
                        hierarchy["CDNAME"],
                        hierarchy["CDUID"],
                        hierarchy["CSDNAME"],
                        hierarchy["CSDUID"],
                    ),
                )
            )

        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Inserting points"):
            await f

    print(f"🎉 Inserted {len(all_points)} points into the DB.")
