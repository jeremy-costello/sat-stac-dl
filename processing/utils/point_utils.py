import random
import geopandas as gpd
from shapely.geometry import Point
import asyncio
from tqdm.asyncio import tqdm_asyncio
from concurrent.futures import ThreadPoolExecutor


CANADA_BBOX = [-141.002, 41.676, -52.63, 83.136]  # lon/lat bounds


def get_random_lon_lat_within_canada():
    """
    Returns a random (lon, lat) within Canada bounds.
    """
    lon = random.uniform(CANADA_BBOX[0], CANADA_BBOX[2])
    lat = random.uniform(CANADA_BBOX[1], CANADA_BBOX[3])
    return lon, lat


async def sample_points_per_geometry(shapefile, id_column, n_points_per_geom=1, seed=None, max_concurrent=100):
    """
    Async version: sample random lon/lat points from each geometry concurrently.

    Parameters:
        shapefile: path to geometry shapefile
        id_column: column name to get ID from
        n_points_per_geom: int, number of points to sample per geometry
        seed: int, optional random seed for reproducibility
        max_concurrent: limit of concurrent geometry sampling tasks

    Returns:
        List of dicts with keys: lon, lat, ID
    """
    if seed is not None:
        random.seed(seed)

    gdf = gpd.read_file(shapefile).to_crs(epsg=4326)

    sem = asyncio.Semaphore(max_concurrent)

    async def process_geometry(row):
        async with sem:
            geom = row.geometry
            row_id = row.get(id_column, None)
            results = []

            if geom.is_empty or row_id is None:
                return results

            minx, miny, maxx, maxy = geom.bounds

            for _ in range(n_points_per_geom):
                for attempt in range(1000):
                    lon = random.uniform(minx, maxx)
                    lat = random.uniform(miny, maxy)
                    point = Point(lon, lat)
                    if geom.contains(point):
                        results.append({"lon": lon, "lat": lat, id_column: row_id})
                        break
                else:
                    print(f"⚠️ Warning: Could not sample point inside geometry {row_id}")
            return results

    tasks = [process_geometry(row) for _, row in gdf.iterrows()]
    results = []

    for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Sampling points"):
        results.extend(await f)

    return results


async def generate_random_points_async(n_points, seed=None, n_workers=8):
    """
    Generate N random points across Canada using ThreadPoolExecutor.
    """
    if seed is not None:
        random.seed(seed)

    def _worker(n):
        chunk = []
        for _ in range(n):
            lon, lat = get_random_lon_lat_within_canada()
            chunk.append({"lon": lon, "lat": lat, "ID": None})
        return chunk

    chunk_size = n_points // n_workers
    results = []

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = [loop.run_in_executor(executor, _worker, chunk_size) for _ in range(n_workers)]
        for f in tqdm_asyncio.as_completed(futures, total=n_workers, desc="Sampling random points"):
            results.extend(await f)
    return results[:n_points]
