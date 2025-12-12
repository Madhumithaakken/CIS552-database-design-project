# plot_sketch_style.py
# Line graph per query matching hand-drawn sketch

import pandas as pd
import matplotlib.pyplot as plt
import os

# -----------------------------
# Paths
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

queries = sorted(raw_df["query"].unique())

OUT_DIR = os.path.join(BASE_DIR, "..", "sketch_style_graphs")
os.makedirs(OUT_DIR, exist_ok=True)

# -----------------------------
# Plot per query
# -----------------------------
for q in queries:
    raw_q = raw_df[raw_df["query"] == q].sort_values("size_mb")
    norm_q = norm_df[norm_df["query"] == q]

    plt.figure()

    # Raw line (1MB → 10MB → 100MB)
    plt.plot(
        raw_q["size_mb"],
        raw_q["median_s"],
        marker="o",
        linestyle="-",
        label="Non-normalized (Raw CSV)"
    )

    # Normalized point (only 100MB)
    if not norm_q.empty:
        plt.scatter(
            [100],
            norm_q["median_s"],
            s=80,
            label="Normalized (100MB)"
        )

    plt.xlabel("Data Size (MB)")
    plt.ylabel("Execution Time (seconds)")
    plt.title(f"Query {q}: Normalized vs Non-normalized Performance")
    plt.xticks([1, 10, 100])
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    out_file = os.path.join(OUT_DIR, f"{q}_sketch_style.png")
    plt.savefig(out_file)
    plt.show()

    print(f"Saved {out_file}")

print("All sketch-style graphs created.")
