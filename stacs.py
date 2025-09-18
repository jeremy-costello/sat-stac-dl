import random
import rioxarray
import stackstac
import planetary_computer
import xarray as xr
from pystac_client import Client


def get_rcm_data(bbox, deg_res, datetime="2019-06-12/2048-01-01"):
    """
    Fetch a random RCM ARD tile from the EODMS STAC API.
    """
    stac_url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac"
    catalog = Client.open(stac_url)

    search = catalog.search(
        collections=["rcm-ard"],
        bbox=bbox,
        datetime=datetime,
        limit=1,
        method="GET"
    )
    items = list(search.items())
    if not items:
        print("Found no RCM items!")
        return None

    assets = [
        "rl",
        "rr",
        "data_mask",
        "local_inc_angle",
        "gamma_to_sigma_ratio",
        "local_contributing_area"
    ]

    arr = stackstac.stack(
        items,
        assets=assets,
        epsg=4326,
        resolution=deg_res,
        bounds_latlon=bbox
    )

    # pick a random time slice
    t_idx = random.randrange(len(arr['time']))
    arr_single = arr.isel(time=[t_idx])

    # rename bands to asset names
    arr_single = arr_single.assign_coords(band=assets)

    return arr_single


def get_landcover_data(bbox, deg_res):
    """
    Fetch Landcover 'classification' data from the Geo.ca STAC API
    for the three years: 2010, 2015, 2020.
    
    Returns:
        xarray DataArray of shape (band=3, y, x)
        with bands named ["landcover-2010", "landcover-2015", "landcover-2020"].
    """
    stac_url = "https://datacube.services.geo.ca/stac/api"
    catalog = Client.open(stac_url)

    search = catalog.search(
        collections=["landcover"],
        bbox=bbox,
        limit=3,
        method="GET"
    )
    items = list(search.items())
    if not items:
        print("No landcover items found!")
        return None

    # sort items by year from their ID (e.g. "landcover-2010")
    items_sorted = sorted(items, key=lambda i: i.id)

    arrs = []
    band_names = []
    for item in items_sorted:
        year = item.id.split("-")[-1]  # extract "2010" from "landcover-2010"
        arr = stackstac.stack(
            [item],
            assets=["classification"],
            epsg=4326,
            resolution=deg_res,
            bounds_latlon=bbox
        )
        # drop time dimension (always 1)
        arr = arr.isel(time=0)
        arrs.append(arr)
        band_names.append(f"landcover-{year}")

    # concatenate into one DataArray with band dimension
    combined = xr.concat(arrs, dim="band", coords="minimal", compat="override")
    combined = combined.assign_coords(band=band_names)

    return combined


def get_hls_data(bbox, deg_res, collection="hls2-s30", datetime="2025-01-01/2030-01-01"):
    """
    Fetch a random HLS tile (Sentinel-2 or Landsat) from the Planetary Computer STAC API.
    """
    stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1"
    catalog = Client.open(stac_url)

    search = catalog.search(
        collections=[collection],
        bbox=bbox,
        datetime=datetime,
        limit=10,
        method="GET"
    )
    items = list(search.items())
    if not items:
        print(f"Found no HLS items in {collection}!")
        return None

    signed_items = [planetary_computer.sign(item) for item in items]

    assets = ["B04", "B03", "B02"]  # Red, Green, Blue

    arr = stackstac.stack(
        signed_items,
        assets=assets,
        epsg=4326,
        resolution=deg_res,
        bounds_latlon=bbox
    )

    t_idx = random.randrange(len(arr['time']))
    arr_single = arr.isel(time=[t_idx])

    # rename bands to colors
    arr_single = arr_single.assign_coords(band=["Red", "Green", "Blue"])

    return arr_single
