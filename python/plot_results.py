# plot_results.py
# FINAL – works with your actual CSV column names

import pandas as pd
import matplotlib.pyplot as plt
import os

# -----------------------------
# File paths
# -----------------------------
BASE_DIR = os.path.dirname(__file__)
RAW_FILE = os.path.join(BASE_DIR, "..", "results_all_tables.csv")
NORM_FILE = os.path.join(BASE_DIR, "..", "results_normalized_100MB.csv")

# -----------------------------
# Load CSV files
# -----------------------------
raw_df = pd.read_csv(RAW_FILE)
norm_df = pd.read_csv(NORM_FILE)

# -----------------------------
# Convert columns to numeric
# -----------------------------
raw_df["median_s"] = raw_df["median_s"].astype(float)
raw_df["size_mb"] = raw_df["size_mb"].astype(int)
norm_df["median_s"] = norm_df["median_s"].astype(float)

# =============================
# GRAPH 1: RAW DATA SCALABILITY
# =============================
plt.figure()

for q in sorted(raw_df["query"].unique()):
    subset = raw_df[raw_df["query"] == q]
    plt.plot(
        subset["size_mb"],
        subset["median_s"],
        marker="o",
        label=q
    )

plt.xlabel("Dataset Size (MB)")
plt.ylabel("Median Execution Time (seconds)")
plt.title("Raw Data Query Scalability")
plt.legend()
plt.grid(True)
plt.tight_layout()

plt.savefig(os.path.join(BASE_DIR, "..", "raw_scalability.png"))
plt.show()

# =====================================
# GRAPH 2: RAW vs NORMALIZED (100MB)
# =====================================
raw_100 = raw_df[raw_df["size_mb"] == 100]

merged = raw_100.merge(
    norm_df,
    on="query",
    suffixes=("_raw", "_norm")
)

plt.figure()

x = range(len(merged))
plt.bar(x, merged["median_s_raw"], width=0.4, label="Raw 100MB")
plt.bar(
    [i + 0.4 for i in x],
    merged["median_s_norm"],
    width=0.4,
    label="Normalized 100MB"
)

plt.xticks([i + 0.2 for i in x], merged["query"])
plt.xlabel("Query")
plt.ylabel("Median Execution Time (seconds)")
plt.title("Raw vs Normalized Performance (100MB)")
plt.legend()
plt.tight_layout()

plt.savefig(os.path.join(BASE_DIR, "..", "raw_vs_normalized_100MB.png"))
plt.show()

print("Graphs generated successfully.")
