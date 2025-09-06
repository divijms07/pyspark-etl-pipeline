from pyspark.sql import SparkSession
import os
def get_spark(app_name="CropYeildPipeline"):
    """
    create and return a Spark session
    """
    python_path = "C:/Users/dimahend/PycharmProjects/Sprint-Project/.venv/Scripts/python.exe"
    os.environ["PYSPARK_PYTHON"] = python_path

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.local.dir","C:/spark-temp")
        .getOrCreate()
    )

    return spark
