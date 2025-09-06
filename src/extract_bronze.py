import os
from utils import get_spark

def main():
    spark = get_spark("CropYield-Bronze")
    spark.sparkContext.setLogLevel("ERROR")

    raw_path = os.path.join("..", "data", "raw", "crop_yield.csv")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"CSV file not found at {raw_path}")

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )

    print(f"Loaded Raw Data \n Rows: {df.count()} \n Columns: {len(df.columns)}")
    df.printSchema()
    df.show(10, truncate=False)

    # Optional: Validate presence of expected columns
    expected_columns = {"N", "P", "K", "Soil_Fertility_Index"}
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    bronze_path = os.path.join("..", "data", "bronze", "crop_yield_v8")
    df.repartition(1).write.mode("overwrite").parquet(bronze_path)

    print(f"Bronze data written to {bronze_path}")
    spark.stop()

if __name__ == "__main__":
    main()

"""
import os
from utils import get_spark
from pyspark.sql.functions import lit


def main():
    spark = get_spark("CropYield-Bronze")
    spark.sparkContext.setLogLevel("ERROR")

    raw_path = os.path.join("..","data", "raw", "crop_yield.csv")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"CSV file not found at {raw_path}")

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )

    print(f"Loaded Raw Data | Rows: {df.count()} | Columns: {len(df.columns)}")
    df.printSchema()
    df.show(10, truncate=False)

    df = df.withColumn("AverageRainfall", lit(500.0))     # Example default value
    df = df.withColumn("Nitrogen", lit(40.0))              # Soil nutrient placeholder
    df = df.withColumn("Phosphorus", lit(25.0))
    df = df.withColumn("Potassium", lit(35.0))

    bronze_path = os.path.join("..","data", "bronze", "crop_yield_v8")
    df.repartition(1).write.mode("overwrite").parquet(bronze_path)
    print(f"Bronze data written to {bronze_path}")
    spark.stop()

if __name__ == "__main__":
    main()
"""