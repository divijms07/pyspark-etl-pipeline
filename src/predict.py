# src/predict.py
import os
from utils import get_spark
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator

def main():
    spark = get_spark("CropYield-Predict")

    # -------------------------
    # 1. Load Model
    # -------------------------
    model_path = os.path.join("..", "models", "linear_regression_crop_yield")
    model = LinearRegressionModel.load(model_path)
    print(f"✅ Model loaded from: {model_path}")

    # -------------------------
    # 2. Load Gold Dataset (or New Data)
    # -------------------------
    gold_path = os.path.join("..", "data", "gold", "crop_yield_v8")
    df = spark.read.parquet(gold_path)
    print("✅ Gold Data Loaded for Prediction")
    print(f"Total Records: {df.count()}")
    df.printSchema()

    # -------------------------
    # 3. Make Predictions
    # -------------------------
    predictions = model.transform(df)
    predictions.select("features", "label", "prediction").show(10, truncate=False)

    # -------------------------
    # 4. Evaluate Predictions
    # -------------------------
    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    print(f"✅ Prediction Evaluation \n RMSE: {rmse:.3f} \n R²: {r2:.3f}")

    # -------------------------
    # 5. Save Predictions
    # -------------------------
    pred_path = os.path.join("..", "data", "predictions", "crop_predictions")
    predictions.repartition(1).write.mode("overwrite").parquet(pred_path)
    print(f"✅ Predictions saved at: {pred_path}")

    spark.stop()

if __name__ == "__main__":
    main()


""""import os
from utils import get_spark
from pyspark.ml.regression import LinearRegressionModel

def main():
    spark = get_spark("CropYield-Predict")

    # -------------------
    # 1. Load Model
    # -------------------
    model_path = os.path.join("..","models", "linear_regression_crop_yield")
    model = LinearRegressionModel.load(model_path)
    print(f"✅ Model loaded from: {model_path}")

    # -------------------
    # 2. Load Gold Dataset (or New Data)
    # -------------------
    gold_path = os.path.join("..","data", "gold", "crop_yield_v8")
    df = spark.read.parquet(gold_path)
    print("✅ Gold Data Loaded for Prediction")

    # -------------------
    # 3. Make Predictions
    # -------------------
    predictions = model.transform(df)
    predictions.select("features", "label", "prediction").show(10, truncate=False)

    # -------------------
    # 4. Save Predictions
    # -------------------
    pred_path = os.path.join("..","data", "predictions", "crop_predictions")
    predictions.repartition(1).write.mode("overwrite").parquet(pred_path)
    print(f"✅ Predictions saved at: {pred_path}")

    spark.stop()

if __name__ == "__main__":
    main()
"""