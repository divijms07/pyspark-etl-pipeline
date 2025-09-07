# src/visualize_gold.py
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    # -------------------------
    # 1. Load Gold CSV
    # -------------------------
    gold_csv_path = os.path.join("..", "data", "gold", "crop_yield_v8_csv")
    csv_files = [f for f in os.listdir(gold_csv_path) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("No CSV file found in gold folder")
    gold_file = os.path.join(gold_csv_path, csv_files[0])

    df = pd.read_csv(gold_file)
    print("✅ Gold Data Loaded for Visualization")
    print(df.head())

    # Set style
    sns.set(style="whitegrid", palette="muted")

    # Create plots folder if not exists
    os.makedirs("../plots", exist_ok=True)


    # -------------------------
    # 1. Impact of Agricultural Support
    # -------------------------
    plt.figure(figsize=(7, 5))
    sns.barplot(
        x="Support_Level", y="Yield_tons_per_hectare",
        data=df, estimator="mean", errorbar=None
    )
    plt.title("Impact of Fertilizer & Irrigation on Yield")
    plt.xlabel("Support Level (0=None, 1=Either, 2=Both)")
    plt.ylabel("Yield (tons per hectare)")
    plt.tight_layout()
    plt.savefig("../plots/yield_by_support.png")

    # -------------------------
    # 2. Yield vs Rainfall Deviation
    # -------------------------
    plt.figure(figsize=(7, 5))
    sns.scatterplot(
        x="Rainfall_Deviation", y="Yield_tons_per_hectare",
        data=df, alpha=0.6
    )
    plt.axvline(0, color="red", linestyle="--", label="Avg Rainfall")
    plt.legend()
    plt.title("Yield vs Rainfall Deviation")
    plt.xlabel("Rainfall Deviation (cm)")
    plt.ylabel("Yield (tons per hectare)")
    plt.tight_layout()
    plt.savefig("../plots/yield_vs_rainfall.png")

    # -------------------------
    # 3. Climate Stress Impact
    # -------------------------
    plt.figure(figsize=(7, 5))
    sns.scatterplot(
        x="Climate_Stress_Index", y="Yield_tons_per_hectare",
        data=df, alpha=0.6
    )
    plt.title("Impact of Climate Stress on Yield")
    plt.xlabel("Climate Stress Index")
    plt.ylabel("Yield (tons per hectare)")
    plt.tight_layout()
    plt.savefig("../plots/yield_vs_climate_stress.png")


    print("✅ Visualizations saved in ../plots/")

if __name__ == "__main__":
    main()
