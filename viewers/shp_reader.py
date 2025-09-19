import geopandas as gpd

# Path to your shapefile
shapefile_path = "./data/inputs/prov_terr"

# Load shapefile
gdf = gpd.read_file(shapefile_path)

# Print total number of subdivisions
print(f"Total subdivisions: {len(gdf)}")

# Optionally, print the first few rows
print(gdf.head())