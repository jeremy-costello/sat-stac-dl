import duckdb
import geopandas as gpd
import pandas as pd
from multiprocessing import Pool, cpu_count
import asyncio
from processing.utils.census_utils import CanadaHierarchy

# Instantiate hierarchy helper once (multiprocess-safe)
hierarchy = CanadaHierarchy()


def _process_chunk(chunk: pd.DataFrame):
    """Process a chunk of joined data and infer hierarchy."""
    processed_rows = []
    for _, row in chunk.iterrows():
        id_ = row["id"]
        if pd.notnull(row.get("CSDUID")):
            pt = {
                "CSDUID": row.get("CSDUID"),
                "CDUID": row.get("CDUID"),
                "PRUID": row.get("PRUID"),
            }
            h = hierarchy.infer_hierarchy(pt)
            processed_rows.append(
                (
                    id_,
                    h["CSDUID"],
                    h["CSDNAME"],
                    h["CDUID"],
                    h["CDNAME"],
                    h["PRUID"],
                    h["PRNAME"],
                )
            )
    return processed_rows


async def update_census_data(
    con: duckdb.DuckDBPyConnection,
    csd_gdf_path: str = "./data/inputs/census_subdiv",
):
    """
    Async: Updates the 'canada_bboxes' table in DuckDB with census data.
    Performs spatial join + hierarchy inference with multiprocessing.
    """

    def _sync_work():
        # --- Load shapefile ---
        print(f"Loading census shapefile from {csd_gdf_path}...")
        csd_gdf = gpd.read_file(csd_gdf_path)

        # Reproject if necessary
        if csd_gdf.crs is None or csd_gdf.crs.to_epsg() != 4326:
            csd_gdf = csd_gdf.to_crs(epsg=4326)

        # --- Get rows needing update ---
        print("Fetching records with missing census subdivision data...")
        df = con.execute(
            """
            SELECT id, lon, lat
            FROM canada_bboxes
            WHERE census_subdiv_id IS NULL
            """
        ).fetchdf()

        if df.empty:
            print("No records with missing census subdivision ID found. Exiting.")
            return

        # --- Build geometries (vectorized, fast) ---
        points = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326",
        )

        # --- Spatial join ---
        print(f"Performing spatial join for {len(points)} points...")
        joined = gpd.sjoin(points, csd_gdf, how="left", predicate="within")

        # --- Hierarchy inference (multiprocess) ---
        print(f"Inferring hierarchy for {len(joined)} joined records...")
        if len(joined) < 1000:
            # small dataset → single process
            processed_data = _process_chunk(joined)
        else:
            num_processes = cpu_count()
            chunk_size = max(1, len(joined) // num_processes)
            chunks = [
                joined.iloc[i : i + chunk_size]
                for i in range(0, len(joined), chunk_size)
            ]
            with Pool(processes=num_processes) as pool:
                processed_data_list = pool.map(_process_chunk, chunks)
            processed_data = [
                item for sublist in processed_data_list for item in sublist
            ]

        # --- Bulk update ---
        if processed_data:
            print(f"Preparing {len(processed_data)} records for bulk update...")
            update_df = pd.DataFrame(
                processed_data,
                columns=[
                    "id",
                    "census_subdiv_id",
                    "census_subdiv",
                    "census_div_id",
                    "census_div",
                    "province_id",
                    "province",
                ],
            )

            con.register("updates", update_df)

            print("Executing bulk update via SQL join...")
            con.execute(
                """
                UPDATE canada_bboxes
                SET census_subdiv_id = u.census_subdiv_id,
                    census_subdiv    = u.census_subdiv,
                    census_div_id    = u.census_div_id,
                    census_div       = u.census_div,
                    province_id      = u.province_id,
                    province         = u.province
                FROM updates AS u
                WHERE canada_bboxes.id = u.id
                """
            )

            print("Bulk update finished.")
        else:
            print("No matching census subdivisions found. No update performed.")

    # Run heavy sync work in a thread so async loop is not blocked
    await asyncio.to_thread(_sync_work)
