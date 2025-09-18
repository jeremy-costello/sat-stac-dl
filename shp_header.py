import geopandas as gpd

def preview_shapefile(shapefile_path):
    """
    Load a shapefile and print its column names and first 5 rows.

    Parameters:
        shapefile_path: str, path to the shapefile (.shp)

    Returns:
        tuple: (list of column names, pandas DataFrame of first 5 rows)
    """
    gdf = gpd.read_file(shapefile_path)
    # print(gdf["DGUID"].head(10))
    columns = gdf.columns.tolist()
    preview = gdf.head(5)
    return columns, preview

# Example usage
if __name__ == "__main__":
    shapefile_path = "./inputs/census_div"
    cols, first5 = preview_shapefile(shapefile_path)
    print("Columns:", cols)
    print("First 5 rows:")
    print(first5)
