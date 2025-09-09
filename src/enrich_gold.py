# src/enrich_gold.py
import os
from utils import get_spark

def main():
    spark = get_spark("CropYield-Gold")

    # ------------------------------
    # Load Silver from Parquet only (fast querying)
    # ------------------------------
    silver_path = os.path.join("..", "data", "silver", "crop_yield_v8_parquet")
    df = spark.read.parquet(silver_path)
    print("✅ Silver Data Loaded (from Parquet)")
    df.printSchema()
    df.show(5, truncate=False)

    # -------------------------
    # 1. Select Relevant Columns (curated dataset)
    # -------------------------
    cols_to_keep = [
        "Region", "Crop", "Soil_Type", "Days_to_Harvest",
        "Rainfall_cm", "Temperature_C", "Yield_tons_per_hectare",
        "Rainfall_Deviation", "Soil_Fertility_Index",
        "N_to_P", "P_to_K", "N_to_K",
        "Support_Level",
        "Weather_Sunny", "Weather_Rainy", "Weather_Cloudy",
        "Climate_Stress_Index"
    ]

    # Keep only columns that actually exist in df
    gold_df = df.select([c for c in cols_to_keep if c in df.columns])

    print("✅ Gold Dataset Ready (Curated Features)")
    gold_df.show(10, truncate=False)

    # -------------------------
    # 2. Save Gold Layer
    # -------------------------
    gold_parquet_path = os.path.join("..", "data", "gold", "crop_yield_v8_parquet")
    gold_csv_path = os.path.join("..", "data", "gold", "crop_yield_v8_csv")

    # Save Parquet (for pipeline queries)
    gold_df.repartition(1).write.mode("overwrite").parquet(gold_parquet_path)

    # Save CSV (for demo visibility)
    gold_df.repartition(1).write.mode("overwrite").option("header", "true").csv(gold_csv_path)

    print(f"✅ Gold data saved at: {gold_parquet_path} (Parquet) and {gold_csv_path} (CSV)")

    spark.stop()

if __name__ == "__main__":
    main()





























