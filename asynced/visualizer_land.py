import duckdb
import matplotlib.pyplot as plt

# Connect to your DuckDB file (adjust path if needed)
con = duckdb.connect("./data/outputs/rcm_ard.duckdb")

# Load the table into a DataFrame
df = con.execute("SELECT * FROM landcover_stats").fetchdf()

class_mapping = {
    "nodata": "No Data",
    "class_1": "Temperate or sub-polar needleleaf forest",
    "class_2": "Sub-polar taiga needleleaf forest",
    "class_3": "Tropical or sub-tropical broadleaf evergreen forest",
    "class_4": "Tropical or sub-tropical broadleaf deciduous forest",
    "class_5": "Temperate or sub-polar broadleaf deciduous forest",
    "class_6": "Mixed forest",
    "class_7": "Tropical or sub-tropical shrubland",
    "class_8": "Temperate or sub-polar shrubland",
    "class_9": "Tropical or sub-tropical grassland",
    "class_10": "Temperate or sub-polar grassland",
    "class_11": "Sub-polar or polar shrubland-lichen-moss",
    "class_12": "Sub-polar or polar grassland-lichen-moss",
    "class_13": "Sub-polar or polar barren-lichen-moss",
    "class_14": "Wetland",
    "class_15": "Cropland",
    "class_16": "Barren land",
    "class_17": "Urban and built-up",
    "class_18": "Water",
    "class_19": "Snow and ice"
}

# Keep only nodata + class_* columns
cols_to_sum = ["nodata"] + [c for c in df.columns if c.startswith("class_")]
class_sums = df[cols_to_sum].sum()

# Rename to human-readable names
class_sums.index = [class_mapping.get(c, c) for c in class_sums.index]

# Convert to percentages
class_percentages = (class_sums / class_sums.sum()) * 100

# Filter out zeros
class_percentages = class_percentages[class_percentages > 0]

# Get distinct colors from tab20
colors = plt.get_cmap("tab20", len(class_percentages))

# Plot
plt.figure(figsize=(8, 8))
wedges, _ = plt.pie(
    class_percentages,
    startangle=90,
    colors=[colors(i) for i in range(len(class_percentages))]
)

# Build legend with percentages (4 decimal places)
legend_labels = [
    f"{name}: {pct:.4f}%" for name, pct in zip(class_percentages.index, class_percentages.values)
]

# Add legend instead of labels around pie
plt.legend(
    wedges,
    legend_labels,
    title="Land Cover Classes",
    loc="center left",
    bbox_to_anchor=(1, 0.5),
    fontsize=9
)

plt.title("Canada Land Cover Distribution")
plt.tight_layout()
plt.show()
