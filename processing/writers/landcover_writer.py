import asyncio
import numpy as np
import rasterio
import pyarrow as pa
from pyproj import Transformer
from concurrent.futures import ProcessPoolExecutor
from tqdm.asyncio import tqdm_asyncio
from processing.utils.landcover_utils import compute_entropy

# Global variables for the worker processes
RASTER_SRC = None
TRANSFORMER = None
NUM_CLASSES = 19
POINT_CRS = 'EPSG:4326'

# Target patch size at "20 m resolution"
PATCH_SIZE = 256           # pixels
TARGET_RES_M = 20          # 20 m per pixel
PATCH_EXTENT_M = PATCH_SIZE * TARGET_RES_M  # 5120 m (~5.1 km)


def create_landcover_table(con):
    class_cols = ", ".join([f"class_{i} BIGINT" for i in range(1, NUM_CLASSES+1)])
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS landcover_stats (
            id INTEGER PRIMARY KEY,
            nodata BIGINT,
            total_count BIGINT,
            {class_cols},
            entropy DOUBLE
        )
    """)


def init_worker(tiff_path):
    """Initializes each worker process by opening the GeoTIFF."""
    global RASTER_SRC, TRANSFORMER
    RASTER_SRC = rasterio.open(tiff_path)
    dst_crs = RASTER_SRC.crs
    TRANSFORMER = Transformer.from_crs(POINT_CRS, dst_crs, always_xy=True)


def process_row_mp(row):
    """Worker function for cropping and stats, run by a process pool."""
    row_id, lon, lat = row

    # Transform center coordinates to TIFF CRS
    cx, cy = TRANSFORMER.transform(lon, lat)

    # Half-size in meters
    half_extent = PATCH_EXTENT_M / 2.0  # ~2560 m

    # Define crop bounds in TIFF CRS (same physical size as 20m patch)
    minx, maxx = cx - half_extent, cx + half_extent
    miny, maxy = cy - half_extent, cy + half_extent

    # Check overlap with dataset bounds
    tif_bounds = RASTER_SRC.bounds
    if not (maxx > tif_bounds.left and minx < tif_bounds.right and
            maxy > tif_bounds.bottom and miny < tif_bounds.top):
        return {
            "id": row_id,
            "nodata": 0,
            "total_count": -1,
            "counts": np.zeros(NUM_CLASSES, dtype=np.int64),
            "entropy": 0.0,
        }

    # Create window in raster pixel coordinates (no resampling)
    window = rasterio.windows.from_bounds(minx, miny, maxx, maxy, RASTER_SRC.transform)

    # Read data at native resolution
    data = RASTER_SRC.read(1, window=window)

    # Count nodata (assuming 0 is nodata for this TIFF)
    nodata_count = np.sum(data == 0)

    # Count classes 1–19 using bincount
    counts_bin = np.bincount(data.flatten(), minlength=NUM_CLASSES + 1)
    counts = counts_bin[1:]

    # Total classified pixels (non-nodata)
    total_count = counts.sum()

    # Entropy (ignore nodata)
    entropy = compute_entropy(counts)

    return {
        "id": row_id,
        "nodata": nodata_count,
        "total_count": total_count,
        "counts": counts,
        "entropy": entropy
    }


async def update_landcover_from_tiff(
        con,
        landcover_tiff_path="./data/inputs/landcover-2020-classification.tif"
):
    loop = asyncio.get_running_loop()
    rows = await loop.run_in_executor(
        None, lambda: con.execute("SELECT id, lon, lat FROM canada_bboxes").fetchall()
    )
    print(f"🌍 Retrieved {len(rows)} rows for landcover stats")

    # Use ProcessPoolExecutor with an initializer
    with ProcessPoolExecutor(initializer=init_worker, initargs=(landcover_tiff_path,)) as executor:
        futures = [
            loop.run_in_executor(executor, process_row_mp, row) for row in rows
        ]
        results = [
            await f
            for f in tqdm_asyncio.as_completed(
                futures, total=len(futures), desc="Landcover crops"
            )
        ]
        
    # Build PyArrow Table and bulk insert
    table_dict = {
        "id": [r["id"] for r in results],
        "nodata": [r["nodata"] for r in results],
        "total_count": [r["total_count"] for r in results],
        "entropy": [r["entropy"] for r in results],
    }
    for i in range(NUM_CLASSES):
        table_dict[f"class_{i+1}"] = [r["counts"][i] for r in results]

    arrow_table = pa.Table.from_pydict(table_dict)

    con.register("landcover_view", arrow_table)
    con.execute("""
        INSERT INTO landcover_stats
        SELECT * FROM landcover_view
    """)
    print(f"✅ Wrote landcover stats for {len(results)} rows")
