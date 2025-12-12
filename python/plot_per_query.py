# plot_per_query.py
# One graph per query: Raw 1MB, Raw 10MB, Raw 100MB, Normalized 100MB

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
# Load data
# -----------------------------
raw_df = pd.read_csv(RAW_FILE)
norm_df = pd.read_csv(NORM_FILE)

raw_df["median_s"] = raw_df["median_s"].astype(float)
raw_df["size_mb"] = raw_df["size_mb"].astype(int)
norm_df["median_s"] = norm_df["median_s"].astype(float)

# -----------------------------
# Queries list
# -----------------------------
queries = sorted(raw_df["query"].unique())

# -----------------------------
# Output directory
# -----------------------------
OUT_DIR = os.path.join(BASE_DIR, "..", "per_query_graphs")
os.makedirs(OUT_DIR, exist_ok=True)

# -----------------------------
# Create one graph per query
# -----------------------------
for q in queries:
    raw_subset = raw_df[raw_df["query"] == q]
    norm_subset = norm_df[norm_df["query"] == q]

    labels = ["Raw 1MB", "Raw 10MB", "Raw 100MB", "Normalized 100MB"]
    values = []

    # Raw sizes
    for size in [1, 10, 100]:
        val = raw_subset[raw_subset["size_mb"] == size]["median_s"].values
        values.append(val[0] if len(val) else None)

    # Normalized 100MB
    norm_val = norm_subset["median_s"].values
    values.append(norm_val[0] if len(norm_val) else None)

    # -----------------------------
    # Plot
    # -----------------------------
    plt.figure()
    plt.bar(labels, values)
    plt.ylabel("Median Execution Time (seconds)")
    plt.title(f"Performance Comparison for {q}")
    plt.grid(axis="y")
    plt.tight_layout()

    out_file = os.path.join(OUT_DIR, f"{q}_comparison.png")
    plt.savefig(out_file)
    plt.show()

    print(f"Saved graph for {q}: {out_file}")

print("All per-query graphs generated.")
