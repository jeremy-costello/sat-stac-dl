import random
import rioxarray
import stackstac
from pystac_client import Client


def get_rcm_data(bbox, deg_res, datetime="2019-06-12/2048-01-01"):
    """
    Fetch a random RCM ARD tile from the EODMS STAC API.
    
    Parameters:
        bbox: [minlon, minlat, maxlon, maxlat] in degrees
        deg_res: (deg_per_pixel_lon, deg_per_pixel_lat)
        datetime: str, temporal range
    
    Returns:
        xarray DataArray of shape (1, assets, tile_size, tile_size)
    """
    stac_url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac"
    catalog = Client.open(stac_url)

    # search RCM ARD collection
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

    return arr_single


def get_landcover_data(bbox, deg_res):
    """
    Fetch Landcover 'classification' data from the Geo.ca STAC API
    for the three years: 2010, 2015, 2020, all from the 'landcover' collection.

    Parameters:
        bbox: [minlon, minlat, maxlon, maxlat] in degrees
        deg_res: (deg_per_pixel_lon, deg_per_pixel_lat)
    
    Returns:
        xarray DataArray of shape (time=3, assets=1, y, x)
    """
    stac_url = "https://datacube.services.geo.ca/stac/api"
    catalog = Client.open(stac_url)

    # search the single collection
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

    assets = [
        "classification"
    ]

    # stack only the 'classification' asset
    arr = stackstac.stack(
        items,
        assets=assets,
        epsg=4326,
        resolution=deg_res,
        bounds_latlon=bbox
    )

    return arr
