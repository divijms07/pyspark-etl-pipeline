# src/transform_silver.py
import os
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from pyspark.sql.window import Window
from utils import get_spark

def main():
    spark = get_spark("CropYield-Silver")

    # ------------------------------
    # CHANGE: Read from Parquet only (faster for pipeline)
    # ------------------------------
    bronze_path = os.path.join("..", "data", "bronze", "crop_yield_v8_parquet")
    df = spark.read.parquet(bronze_path)
    print("✅ Bronze Data Loaded (from Parquet)")
    df.printSchema()
    df.show(10, truncate=False)

    # -------------------------
    # 1. Count Missing Values
    # -------------------------
    df.select([F.sum(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]).show()

    # -------------------------
    # 2. Handle Missing Values
    # -------------------------
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    for col in numeric_cols:
        try:
            mean_val = df.select(F.mean(F.col(col))).first()[0]
            if mean_val is not None:
                df = df.na.fill({col: mean_val})
        except Exception as e:
            print(f"⚠️ Skipping column '{col}' due to error: {e}")

    # -------------------------
    # 3. Normalize & Round Units
    # -------------------------
    if "Rainfall_mm" in df.columns:
        df = df.withColumn("Rainfall_mm", F.round(F.col("Rainfall_mm"), 2))
        df = df.withColumn("Rainfall_cm", F.round(F.col("Rainfall_mm") / 10.0, 2))

    if "Temperature_Celsius" in df.columns:
        df = df.withColumn("Temperature_Celsius", F.round(F.col("Temperature_Celsius"), 2))
        df = df.withColumnRenamed("Temperature_Celsius", "Temperature_C")

    # -------------------------
    # 4. Cast Boolean to Integer
    # -------------------------
    if "Fertilizer_Used" in df.columns:
        df = df.withColumn("Fertilizer_Used", F.col("Fertilizer_Used").cast("boolean").cast("int"))
    if "Irrigation_Used" in df.columns:
        df = df.withColumn("Irrigation_Used", F.col("Irrigation_Used").cast("boolean").cast("int"))

    # -------------------------
    # 5. Feature Engineering
    # -------------------------

    # (a) Rainfall Deviation: shows water stress
    if "Rainfall_cm" in df.columns and "Region" in df.columns:
        windowSpec = Window.partitionBy("Region")
        df = df.withColumn("Avg_Rainfall", F.round(F.avg(F.col("Rainfall_cm")).over(windowSpec), 2))
        df = df.withColumn("Rainfall_Deviation", F.round(F.col("Rainfall_cm") - F.col("Avg_Rainfall"), 2))

    # (b) Soil Fertility Index (recomputed)
    if all(c in df.columns for c in ["N", "P", "K"]):
        df = df.withColumn(
            "Soil_Fertility_Index",
            F.round((F.col("N") + F.col("P") + F.col("K")) / 3, 2)
        )

        # (c) NPK Ratio Features
        df = df.withColumn("N_to_P", F.round(F.col("N") / F.when(F.col("P") != 0, F.col("P")), 2))
        df = df.withColumn("P_to_K", F.round(F.col("P") / F.when(F.col("K") != 0, F.col("K")), 2))
        df = df.withColumn("N_to_K", F.round(F.col("N") / F.when(F.col("K") != 0, F.col("K")), 2))

    # (d) Agricultural Support (Fertilizer + Irrigation)
    if "Fertilizer_Used" in df.columns and "Irrigation_Used" in df.columns:
        df = df.withColumn("Support_Level", F.col("Fertilizer_Used") + F.col("Irrigation_Used"))

    # (e1) Weather Encoding (Sunny, Rainy, Cloudy)
    if "Weather_Condition" in df.columns:
        weather_values = ["Sunny", "Rainy", "Cloudy"]
        for val in weather_values:
            df = df.withColumn(f"Weather_{val}", F.when(F.col("Weather_Condition") == val, 1).otherwise(0))

    # (e2) Climate Stress Index
    if all(c in df.columns for c in ["Rainfall_Deviation", "Temperature_C", "Days_to_Harvest"]):
        df = df.withColumn(
            "Climate_Stress_Index",
            F.round((F.col("Rainfall_Deviation") * F.col("Temperature_C")) / F.col("Days_to_Harvest"), 2)
        )

    # Preview new engineered features
    df.show(5, truncate=False)


    # -------------------------
    # 6. Save Silver Layer (both Parquet + CSV)
    # -------------------------
    silver_parquet_path = os.path.join("..", "data", "silver", "crop_yield_v8_parquet")
    silver_csv_path = os.path.join("..", "data", "silver", "crop_yield_v8_csv")

    (
        df.write
        .mode("overwrite")
        .partitionBy("Crop", "Region")
        .parquet(silver_parquet_path)   # For pipeline queries
    )

    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(silver_csv_path)          # For demo visibility
    )

    print(f"✅ Silver data saved at: {silver_parquet_path} (Parquet) and {silver_csv_path} (CSV)")
    spark.stop()

if __name__ == "__main__":
    main()




