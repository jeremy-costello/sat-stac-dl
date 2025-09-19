import geopandas as gpd
from shapely.geometry import box

def features_from_bbox(bbox, shapefile_path, columns=None, find_nearest=False):
    """
    Find which features in a shapefile intersect a bbox.
    Optionally return specified columns and/or the nearest feature if none intersect.

    Parameters:
        bbox: [minx, miny, maxx, maxy] in the same CRS as shapefile
        shapefile_path: path to the shapefile
        columns: list of column names to return (default: all columns)
        find_nearest: bool, if True, return nearest feature when no intersection

    Returns:
        List of dictionaries with requested columns for intersecting (or nearest) features.
        Each dictionary includes a 'match_type' key: "intersection" or "nearest".
        Empty list if nothing found and find_nearest=False.
    """
    # Load shapefile
    gdf = gpd.read_file(shapefile_path)

    # Ensure CRS is WGS84 (EPSG:4326) if bbox is in degrees
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Build bbox polygon
    bbox_poly = box(*bbox)

    # Find intersecting features
    matches = gdf[gdf.intersects(bbox_poly)]

    if not matches.empty:
        match_type = "intersection"
    elif find_nearest:
        # No intersection: find nearest feature
        gdf["dist_to_bbox"] = gdf.geometry.apply(lambda g: g.distance(bbox_poly))
        nearest = gdf.loc[gdf["dist_to_bbox"].idxmin()]
        matches = gdf.loc[[nearest.name]]  # keep as GeoDataFrame
        match_type = "nearest"
    else:
        return []

    # Select requested columns
    if columns is not None:
        cols_to_return = [c for c in columns if c in matches.columns]
    else:
        cols_to_return = matches.columns.tolist()

    # Convert to list of dicts and add match_type
    results = matches[cols_to_return].to_dict(orient="records")
    for r in results:
        r["match_type"] = match_type

    return results
