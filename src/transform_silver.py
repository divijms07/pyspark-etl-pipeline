# src/transform_silver.py
import os
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from pyspark.sql.window import Window
from utils import get_spark

def main():
    spark = get_spark("CropYield-Silver")

    # Load Bronze parquet
    bronze_path = os.path.join("..", "data", "bronze", "crop_yield_v8")
    df = spark.read.parquet(bronze_path)
    print("✅ Bronze Data Loaded")
    df.printSchema()
    df.show(10, truncate=False)

    # -------------------------
    # 1. Count Missing Values
    # -------------------------
    # Count missing/null values in each column
    df.select([F.sum(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]).show()

    # -------------------------
    # 2. Handle Missing Values
    # -------------------------
    # Example: fill numeric cols with mean
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
    # Round Rainfall_mm and Temperature_Celsius to 2 decimal places
    if "Rainfall_mm" in df.columns:
        df = df.withColumn("Rainfall_mm", F.round(F.col("Rainfall_mm"), 2))
        df = df.withColumn("Rainfall_cm", F.round(F.col("Rainfall_mm") / 10.0, 2))

    if "Temperature_Celsius" in df.columns:
        df = df.withColumn("Temperature_Celsius", F.round(F.col("Temperature_Celsius"), 2))
        df = df.withColumnRenamed("Temperature_Celsius", "Temperature_C")

    # -------------------------
    # 4. Cast Boolean to Integer
    # -------------------------
    # Cast boolean columns to integers (True → 1, False → 0)
    if "Fertilizer_Used" in df.columns:
        df = df.withColumn("Fertilizer_Used", F.col("Fertilizer_Used").cast("boolean").cast("int"))
    if "Irrigation_Used" in df.columns:
        df = df.withColumn("Irrigation_Used", F.col("Irrigation_Used").cast("boolean").cast("int"))

    # -------------------------
    # 5. Feature Engineering
    # -------------------------
    # Measures how actual rainfall deviates from the average per region
    if "Rainfall_cm" in df.columns and "Region" in df.columns:
        windowSpec = Window.partitionBy("Region")
        df = df.withColumn("Avg_Rainfall", F.round(F.avg(F.col("Rainfall_cm")).over(windowSpec), 2))
        df = df.withColumn("Rainfall_Deviation", F.round(F.col("Rainfall_cm") - F.col("Avg_Rainfall"), 2))

    # Computes a simple average of three key soil nutrients
    if all(c in df.columns for c in ["N", "P", "K"]):
        df = df.withColumn(
            "Soil_Fertility_Index",
            F.round((F.col("N") + F.col("P") + F.col("K")) / 3, 2)
        )

    df.show(5, truncate=False)

    # -------------------------
    # 6. Save Silver Layer (partitioned)
    # -------------------------
    silver_path = os.path.join("..", "data", "silver", "crop_yield_v8")
    (
        df.write
        .mode("overwrite")
        .partitionBy("Crop", "Region")  # adjust if your dataset has these cols
        .parquet(silver_path)
    )
    print(f"✅ Silver data saved at: {silver_path}")
    spark.stop()

if __name__ == "__main__":
    main()





"""
import os
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from utils import get_spark

def main():
    spark = get_spark("CropYield-Silver")

    # Load Bronze parquet
    bronze_path = os.path.join("..","data", "bronze", "crop_yield_v8")
    df = spark.read.parquet(bronze_path)

    print("✅ Bronze Data Loaded")
    df.printSchema()
    df.show(10, truncate=False)

    # -------------------
    # 1. Handle Missing Values
    # -------------------
    # Example: fill numeric cols with mean
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    for col in numeric_cols:
        try:
            mean_val = df.select(F.mean(F.col(col))).first()[0]
            if mean_val is not None:
                df = df.na.fill({col: mean_val})
        except Exception as e:
            print(f"⚠️ Skipping column '{col}' due to error: {e}")


    # -------------------
    # 2. Normalize Units
    # -------------------
    if "Rainfall_mm" in df.columns:
        df = df.withColumn("Rainfall_cm", F.col("Rainfall_mm") / 10.0)  # mm → cm
    if "Temperature_Celsius" in df.columns:
        # assume already in Celsius, but if in Fahrenheit:
        # df = df.withColumn("Temperature_C", (F.col("Temperature") - 32) * 5/9)
        df = df.withColumnRenamed("Temperature_Celsius", "Temperature_C")

    # -------------------
    # 3. Feature Engineering
    # -------------------

    ## Measures how actual rainfall deviates from the average.
    if all(c in df.columns for c in ["Rainfall", "AverageRainfall"]):
        df = df.withColumn("Rainfall_Deviation", F.col("Rainfall") - F.col("AverageRainfall"))

    ## Computes a simple average of three key soil nutrients.
    if all(c in df.columns for c in ["Nitrogen", "Phosphorus", "Potassium"]):
        df = df.withColumn(
            "Soil_Fertility_Index",
            (F.col("Nitrogen") + F.col("Phosphorus") + F.col("Potassium")) / 3
        )

    # -------------------
    # 4. Save Silver Layer (partitioned)
    # -------------------
    silver_path = os.path.join("..","data", "silver", "crop_yield_v8")
    (
        df.write
        .mode("overwrite")
        .partitionBy("Crop", "Region")  # adjust if your dataset has these cols
        .parquet(silver_path)
    )

    print(f"✅ Silver data saved at: {silver_path}")
    spark.stop()

if __name__ == "__main__":
    main()
"""