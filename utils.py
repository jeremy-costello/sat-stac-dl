import random
import math
import rasterio


CANADA_BBOX = [-141.002, 41.676, -52.63, 83.136]  # lon/lat bounds


def get_bbox_within_canada(resolution_m: int, tile_size: int):
    # pick a random latitude/longitude
    lon = random.uniform(CANADA_BBOX[0], CANADA_BBOX[2])
    lat = random.uniform(CANADA_BBOX[1], CANADA_BBOX[3])

    # meters per degree
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = 111_320.0 * math.cos(math.radians(lat))

    # convert resolution to degrees
    deg_per_pixel_lat = resolution_m / meters_per_deg_lat
    deg_per_pixel_lon = resolution_m / meters_per_deg_lon

    # half-extent = (tile_size/2) * resolution
    half_lat = ((tile_size - 1) / 2) * deg_per_pixel_lat
    half_lon = ((tile_size - 1) / 2) * deg_per_pixel_lon

    # bbox in lon/lat
    minlon = lon - half_lon
    maxlon = lon + half_lon
    minlat = lat - half_lat
    maxlat = lat + half_lat

    return {
        "bbox": [minlon, minlat, maxlon, maxlat],
        "deg_resolution": (deg_per_pixel_lon, deg_per_pixel_lat)
    }


# Helper: save raster + band names
def save_with_bandnames(arr, filename, band_names=None):
    arr.rio.to_raster(filename)
    if band_names is not None:
        with rasterio.open(filename, "r+") as dst:
            for i, name in enumerate(band_names, start=1):
                dst.set_band_description(i, str(name))