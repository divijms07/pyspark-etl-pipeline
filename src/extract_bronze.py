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

    # ------------------------------
    # CHANGE: Save both Parquet (for pipeline) and CSV (for demo)
    # ------------------------------
    bronze_parquet_path = os.path.join("..", "data", "bronze", "crop_yield_v8_parquet")
    bronze_csv_path = os.path.join("..", "data", "bronze", "crop_yield_v8_csv")

    df.repartition(1).write.mode("overwrite").parquet(bronze_parquet_path)
    df.repartition(1).write.mode("overwrite").option("header", "true").csv(bronze_csv_path)

    print(f"Bronze data written to: {bronze_parquet_path} (Parquet) and {bronze_csv_path} (CSV)")
    spark.stop()

if __name__ == "__main__":
    main()
