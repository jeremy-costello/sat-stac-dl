import pandas as pd
import aiohttp
from pathlib import Path
import rasterio
from rasterio.windows import from_bounds
from rasterio.fill import fillnodata
import numpy as np
from pyproj import Transformer
from tqdm.asyncio import tqdm
import os

# --- CONFIG ---
RCM_TABLE_SOURCE = "rcm_ard_items"
RCM_TABLE_PROPS = "rcm_ard_properties"
RCM_TABLE_TARGET = "rcm_ard_tiles"
BBOX_TABLE = "canada_bboxes"
OUTPUT_DIR = Path("./data/outputs/rcm_tiles")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
FILTER_PRUID = None
FILTER_CDUID = 1001
BAND_MAP = {"rl": "RL", "rr": "RR"}  # asset mappings
NODATA_CUTOFF = 0.01


async def create_rcm_ard_tiles_table(con):
    # Create / reset target table
    con.execute(f"""
        CREATE OR REPLACE TABLE {RCM_TABLE_TARGET} (
            id INTEGER,
            item TEXT,
            rl_nodata_pct DOUBLE,
            rr_nodata_pct DOUBLE,
            filepath TEXT
        );
    """)


async def download_file(session, url, out_path):
    async with session.get(url) as resp:
        resp.raise_for_status()
        with open(out_path, "wb") as f:
            async for chunk in resp.content.iter_chunked(1024 * 1024):
                f.write(chunk)


async def download_bands(item, datetime, order_key, session):
    """Download RL and RR tif for one item into its own folder."""
    date = pd.to_datetime(datetime)
    yyyy, mm, dd = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
    base = order_key.replace("_CH_CV_MLC", "")

    # Create folder for this item
    item_dir = OUTPUT_DIR / item
    item_dir.mkdir(parents=True, exist_ok=True)

    out_files = {}
    for k, band in BAND_MAP.items():
        url = (
            f"https://rcm-ceos-ard.s3.ca-central-1.amazonaws.com/MLC/"
            f"{yyyy}/{mm}/{dd}/{order_key}/{base}_{band}.tif"
        )
        out_path = item_dir / f"{item}_{band}.tif"
        if not out_path.exists():
            await download_file(session, url, out_path)
        out_files[k] = out_path
    return out_files


def combine_bands(rr_path, rl_path, out_path):
    """Combine RL and RR into one multi-band tiff."""
    with rasterio.open(rr_path) as rr, rasterio.open(rl_path) as rl:
        meta = rr.meta.copy()
        meta.update(count=2)
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(rr.read(1), 1)
            dst.write(rl.read(1), 2)
    return out_path


def crop_tiff(input_tif, bbox_latlon, out_path):
    with rasterio.open(input_tif) as src:
        transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
        minx, miny = transformer.transform(bbox_latlon[0], bbox_latlon[1])
        maxx, maxy = transformer.transform(bbox_latlon[2], bbox_latlon[3])
        bbox = [minx, miny, maxx, maxy]

        window = from_bounds(*bbox, transform=src.transform)
        data = src.read(window=window)
        meta = src.meta.copy()
        meta.update(
            width=data.shape[2], height=data.shape[1],
            transform=src.window_transform(window)
        )

        # --- Compute nodata fraction (0–1) ---
        nodata_val = src.nodata if src.nodata is not None else 0
        nodata_frac = [(np.count_nonzero(b == nodata_val) / b.size) for b in data]

        # --- Check cutoff ---
        if any(frac > NODATA_CUTOFF for frac in nodata_frac):
            return nodata_frac, None  # skip saving, return fractions only

        # --- Fill nodata if below cutoff ---
        filled_data = np.empty_like(data)
        for i in range(data.shape[0]):  # loop over bands
            band = data[i].astype(np.float32)
            mask = band != nodata_val
            filled_band = fillnodata(
                band,
                mask=mask.astype(np.uint8),
                max_search_distance=100,
                smoothing_iterations=0,
                nodata=nodata_val
            )
            filled_data[i] = filled_band

        # Save filled tif
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(filled_data)

    return nodata_frac, str(out_path)


async def download_rcm_tiles(con):
    # Step 1: get unique items + their datetime & order_key
    filter_clause = ""
    if FILTER_PRUID:
        filter_clause += f"AND c.province_id = {FILTER_PRUID}"
    elif FILTER_CDUID:
        filter_clause += f"AND c.census_div_id = {FILTER_CDUID}"
        
    df_items = con.execute(f"""
        SELECT DISTINCT unnest(r.items) as item
        FROM {RCM_TABLE_SOURCE} r
        JOIN {BBOX_TABLE} c ON r.id = c.id
        WHERE array_length(r.items) > 0
        {filter_clause}
    """).df()

    # Get datetime/order_key mapping for each item
    props = con.execute(f"SELECT item, datetime, order_key FROM {RCM_TABLE_PROPS}").df()
    props_map = {row.item: (row.datetime, row.order_key) for row in props.itertuples()}

    async with aiohttp.ClientSession() as session:
        for item in tqdm(df_items["item"], desc="Processing items"):
            if item not in props_map:
                print(f"⚠️ Skipping {item}, no properties found.")
                continue

            datetime, order_key = props_map[item]
            band_files = await download_bands(item, datetime, order_key, session)

            # merged path inside item folder
            item_dir = OUTPUT_DIR / item
            merged_path = item_dir / f"{item}_merged.tif"
            if not merged_path.exists():
                combine_bands(band_files["rr"], band_files["rl"], merged_path)
                # Remove single-band files after merge
                for f in band_files.values():
                    try:
                        os.remove(f)
                    except FileNotFoundError:
                        pass

            # Get all IDs using this item
            ids = con.execute(f"""
                SELECT id FROM {RCM_TABLE_SOURCE} 
                WHERE list_contains(items, '{item}')
            """).df()["id"]

            for row_id in tqdm(ids, desc=f"Cropping {item}", leave=False):
                bbox = con.execute(f"SELECT bbox FROM {BBOX_TABLE} WHERE id = {row_id}").fetchone()[0]
                bbox = [float(x) for x in bbox.split(",")] if isinstance(bbox, str) else bbox

                out_crop = item_dir / f"{item}_{row_id}.tif"
                nodata_fracs, out_path = crop_tiff(merged_path, bbox, out_crop)

                # Insert into target table
                con.execute(f"""
                    INSERT INTO {RCM_TABLE_TARGET} (id, item, rl_nodata_pct, rr_nodata_pct, filepath)
                    VALUES (?, ?, ?, ?, ?)
                """, [row_id, item, nodata_fracs[1], nodata_fracs[0], out_path])

            # Remove merged file after all crops are done
            try:
                os.remove(merged_path)
            except FileNotFoundError:
                pass
