import math


def get_bbox_from_point(lon: float, lat: float, resolution_m: int, tile_size: int):
    """
    Generate a bbox centered at the given lon/lat.

    Returns:
        dict with 'bbox' (minlon, minlat, maxlon, maxlat) and 'deg_resolution' (deg_per_pixel_lon, deg_per_pixel_lat)
    """
    # meters per degree
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = 111_320.0 * math.cos(math.radians(lat))

    # convert resolution to degrees
    deg_per_pixel_lat = resolution_m / meters_per_deg_lat
    deg_per_pixel_lon = resolution_m / meters_per_deg_lon

    # half-extent = (tile_size-1)/2 pixels
    half_lat = ((tile_size - 1) / 2) * deg_per_pixel_lat
    half_lon = ((tile_size - 1) / 2) * deg_per_pixel_lon

    # bbox in lon/lat
    minlon = lon - half_lon
    maxlon = lon + half_lon
    minlat = lat - half_lat
    maxlat = lat + half_lat

    return {
        "bbox": (minlon, minlat, maxlon, maxlat),
        "deg_resolution": (deg_per_pixel_lon, deg_per_pixel_lat)
    }
