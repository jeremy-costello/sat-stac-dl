import json
import random
import geopandas as gpd
from shapely.geometry import Point
import asyncio
from tqdm.asyncio import tqdm_asyncio
from concurrent.futures import ThreadPoolExecutor
from shared.utils import get_random_lon_lat_within_canada


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

    loop = asyncio.get_running_loop()
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


class CanadaHierarchy:
    def __init__(self, pr_map="./data/inputs/PR_mapping.json",
                       cd_map="./data/inputs/CD_mapping.json",
                       csd_map="./data/inputs/CSD_mapping.json"):
        # Load mappings once
        with open(pr_map) as f:
            self.prid_to_prname = json.load(f)
        with open(cd_map) as f:
            self.cdid_to_cdname = json.load(f)
        with open(csd_map) as f:
            self.csdid_to_csdname = json.load(f)

    def infer_hierarchy(self, pt):
        """
        Given a sampled point dict, infer the hierarchical IDs and names
        for province, census division, and census subdivision.
        Returns a dictionary with keys: PRUID, PRNAME, CDUID, CDNAME, CSDUID, CSDNAME.
        Missing values are set to None.
        """
        csd_id = pt.get("CSDUID")
        cd_id = pt.get("CDUID")
        pr_id = pt.get("PRUID")

        hierarchy = {
            "PRUID": None,
            "PRNAME": None,
            "CDUID": None,
            "CDNAME": None,
            "CSDUID": None,
            "CSDNAME": None
        }

        if csd_id is not None:
            hierarchy["CSDUID"] = csd_id
            hierarchy["CSDNAME"] = self.csdid_to_csdname.get(str(csd_id))
            cd_id = int(str(csd_id)[:4])
            hierarchy["CDUID"] = cd_id
            hierarchy["CDNAME"] = self.cdid_to_cdname.get(str(cd_id))
            pr_id = int(str(csd_id)[:2])
            hierarchy["PRUID"] = pr_id
            hierarchy["PRNAME"] = self.prid_to_prname.get(str(pr_id))

        elif cd_id is not None:
            hierarchy["CDUID"] = cd_id
            hierarchy["CDNAME"] = self.cdid_to_cdname.get(str(cd_id))
            pr_id = int(str(cd_id)[:2])
            hierarchy["PRUID"] = pr_id
            hierarchy["PRNAME"] = self.prid_to_prname.get(str(pr_id))

        elif pr_id is not None:
            hierarchy["PRUID"] = pr_id
            hierarchy["PRNAME"] = self.prid_to_prname.get(str(pr_id))

        return hierarchy
