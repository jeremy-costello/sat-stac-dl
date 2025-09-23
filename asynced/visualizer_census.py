import duckdb
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import squarify

# Connect to DuckDB
con = duckdb.connect("./data/outputs/rcm_ard_tiles.duckdb")

# --- Province Pie Chart ---
province_data = con.execute("""
    SELECT province, COUNT(*) AS cnt
    FROM canada_bboxes
    GROUP BY province
""").fetchdf()

plt.figure(figsize=(8,8))
colors = plt.cm.tab20.colors[:len(province_data)]
wedges, _ = plt.pie(province_data["cnt"], colors=colors, startangle=90)

total = province_data["cnt"].sum()
legend_labels = [
    f"{prov} ({count/total:.1%})"
    for prov, count in zip(province_data["province"], province_data["cnt"])
]

plt.legend(
    wedges,
    legend_labels,
    title="Provinces",
    loc="center left",
    bbox_to_anchor=(1, 0.5),
    fontsize=10
)
plt.title("Distribution by Province")
plt.tight_layout()
plt.show()


# --- Census Divisions (CD) Treemap ---
cd_data = con.execute("""
    SELECT province, census_div, COUNT(*) AS cnt
    FROM canada_bboxes
    WHERE census_div IS NOT NULL
    GROUP BY province, census_div
    ORDER BY cnt DESC
""").fetchdf()

top_cd = cd_data.head(20).copy()  # top 20 items
top_cd["label"] = top_cd.apply(lambda x: f"{x['census_div']} ({x['province']})", axis=1)

plt.figure(figsize=(16,10))
colors = plt.cm.tab20.colors[:len(top_cd)]  # tab20 for 20 items

squarify.plot(
    sizes=top_cd["cnt"],
    label=None,  # no text on squares
    color=colors,
    alpha=0.8,
    pad=True
)

# Legend to the right
legend_patches = [mpatches.Patch(color=colors[i], label=top_cd["label"].iloc[i]) for i in range(len(top_cd))]
plt.legend(handles=legend_patches, title="Census Divisions", bbox_to_anchor=(1, 0.5), loc="center left",
           fontsize=8, framealpha=0.8)

plt.title("Top 20 Census Divisions Treemap (with Province)")
plt.axis('off')
plt.tight_layout()
plt.show()


# --- Census Subdivisions (CSD) Treemap ---
csd_data = con.execute("""
    SELECT province, census_div, census_subdiv, COUNT(*) AS cnt
    FROM canada_bboxes
    WHERE census_subdiv IS NOT NULL
    GROUP BY province, census_div, census_subdiv
    ORDER BY cnt DESC
""").fetchdf()

top_csd = csd_data.head(20).copy()  # top 20 items
top_csd["label"] = top_csd.apply(lambda x: f"{x['census_subdiv']} ({x['census_div']}, {x['province']})", axis=1)

plt.figure(figsize=(16,10))
colors = plt.cm.tab20.colors[:len(top_csd)]  # tab20 for 20 items

squarify.plot(
    sizes=top_csd["cnt"],
    label=None,  # no text on squares
    color=colors,
    alpha=0.8,
    pad=True
)

legend_patches = [mpatches.Patch(color=colors[i], label=top_csd["label"].iloc[i]) for i in range(len(top_csd))]
plt.legend(handles=legend_patches, title="Census Subdivisions", bbox_to_anchor=(1, 0.5), loc="center left",
           fontsize=6, framealpha=0.8)

plt.title("Top 20 Census Subdivisions Treemap (with Division and Province)")
plt.axis('off')
plt.tight_layout()
plt.show()
