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
import random
from collections import defaultdict

# --- CONFIG ---
RCM_TABLE_SOURCE = "rcm_ard_items"
RCM_TABLE_PROPS = "rcm_ard_properties"
RCM_TABLE_TARGET = "rcm_ard_tiles"
BBOX_TABLE = "canada_bboxes"
OUTPUT_DIR = Path("./data/outputs/rcm_tiles")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
FILTER_PRUID = None
FILTER_CDUID = 1006
NODATA_CUTOFF = 0.01
ITEMS_PER_ID = 5
CROP_SIZE = 256
# asset mappings
BAND_MAP = {
    "rl": "RL",
    "rr": "RR"
}


async def create_rcm_ard_tiles_table(con):
    # Build dynamic nodata columns
    nodata_cols = ",\n".join([f"{key}_nodata_pct DOUBLE" for key in BAND_MAP.keys()])
    
    con.execute(f"""
        CREATE OR REPLACE TABLE {RCM_TABLE_TARGET} (
            id INTEGER,
            item TEXT,
            {nodata_cols},
            filepath TEXT,
            height INTEGER,
            width INTEGER
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


def combine_bands(band_paths: dict, out_path):
    """Combine bands into one multi-band tiff with band names from BAND_MAP."""
    first_key = next(iter(band_paths))
    with rasterio.open(band_paths[first_key]) as src0:
        meta = src0.meta.copy()
        meta.update(count=len(band_paths))

    with rasterio.open(out_path, "w", **meta) as dst:
        for i, (key, path) in enumerate(band_paths.items(), start=1):
            with rasterio.open(path) as src:
                dst.write(src.read(1), i)
                dst.set_band_description(i, key)
    return out_path


def crop_tiff(input_tif, lon, lat, out_path, crop_size=CROP_SIZE):
    with rasterio.open(input_tif) as src:
        # Transform lon/lat to raster CRS
        transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
        x, y = transformer.transform(lon, lat)

        # Convert to row/col in raster grid
        row, col = src.index(x, y)

        # Define window centered on (row, col)
        half = crop_size // 2
        window = rasterio.windows.Window(
            col_off=col - half,
            row_off=row - half,
            width=crop_size,
            height=crop_size
        )

        # Read data in window
        data = src.read(window=window, boundless=True, fill_value=src.nodata)
        meta = src.meta.copy()
        meta.update(
            width=crop_size,
            height=crop_size,
            transform=src.window_transform(window)
        )

        # --- Compute nodata fraction (0–1) ---
        nodata_val = src.nodata if src.nodata is not None else 0
        nodata_frac = [(np.count_nonzero(b == nodata_val) / b.size) for b in data]

        # --- Check cutoff ---
        if any(frac > NODATA_CUTOFF for frac in nodata_frac):
            return nodata_frac, None, data.shape[1], data.shape[2]

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

    return nodata_frac, str(out_path), data.shape[1], data.shape[2]


async def download_rcm_tiles(con):
    # Step 1: get unique items + their datetime & order_key
    filter_clause = ""
    if FILTER_CDUID:
        filter_clause += f"AND c.census_div_id = {FILTER_CDUID}"
    elif FILTER_PRUID:
        filter_clause += f"AND c.province_id = {FILTER_PRUID}"
        
    df_ids = con.execute(f"""
        SELECT r.id, r.items
        FROM {RCM_TABLE_SOURCE} r
        JOIN {BBOX_TABLE} c ON r.id = c.id
        WHERE array_length(r.items) > 0
        {filter_clause}
    """).df()

    id_to_items = dict()
    for row in df_ids.itertuples():
        row_id = row.id
        items = list(row.items)
        if ITEMS_PER_ID is not None:
            sampled = random.sample(items, min(ITEMS_PER_ID, len(items)))
            id_to_items[row_id] = sampled
        else:
            id_to_items[row_id] = items
    
    item_to_ids = defaultdict(list)
    for row_id, items in id_to_items.items():
        for item in items:
            item_to_ids[item].append(row_id)

    # Get datetime/order_key mapping for each item
    props = con.execute(f"SELECT item, datetime, order_key FROM {RCM_TABLE_PROPS}").df()
    props_map = {row.item: (row.datetime, row.order_key) for row in props.itertuples()}

    async with aiohttp.ClientSession() as session:
        for item in tqdm(item_to_ids.keys(), desc="Processing items"):
            if item not in props_map:
                print(f"⚠️ Skipping {item}, no properties found.")
                continue

            datetime, order_key = props_map[item]
            band_files = await download_bands(item, datetime, order_key, session)

            # merged path inside item folder
            item_dir = OUTPUT_DIR / item
            merged_path = item_dir / f"{item}_merged.tif"
            if not merged_path.exists():
                combine_bands(band_files, merged_path)
                # Remove single-band files after merge
                for f in band_files.values():
                    try:
                        os.remove(f)
                    except FileNotFoundError:
                        pass

            # Process only the IDs that sampled this item
            for row_id in tqdm(item_to_ids[item], desc=f"Cropping {item}", leave=False):
                lon, lat = con.execute(
                    f"SELECT lon, lat FROM {BBOX_TABLE} WHERE id = {row_id}"
                ).fetchone()

                out_crop = item_dir / f"{item}_{row_id}.tif"
                nodata_fracs, out_path, height, width = crop_tiff(
                    merged_path, lon, lat, out_crop, crop_size=CROP_SIZE
                )

                # Build dynamic column/value list for nodata fractions
                nodata_cols = [f"{key}_nodata_pct" for key in BAND_MAP.keys()]
                nodata_vals = [nodata_fracs[i] for i in range(len(BAND_MAP))]

                con.execute(f"""
                    INSERT INTO {RCM_TABLE_TARGET} (
                        id, item, {", ".join(nodata_cols)}, filepath, height, width
                    ) VALUES (
                        ?, ?, {", ".join(["?"] * len(nodata_cols))}, ?, ?, ?
                    )
                """, [row_id, item, *nodata_vals, out_path, height, width])

            # Remove merged file after all crops are done
            try:
                os.remove(merged_path)
            except FileNotFoundError:
                pass
