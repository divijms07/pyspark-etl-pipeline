# src/train_model.py
import os
from utils import get_spark
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

def main():
    spark = get_spark("CropYield-TrainModel")

    # Load Gold dataset
    gold_path = os.path.join("..", "data", "gold", "crop_yield_v8")
    df = spark.read.parquet(gold_path)
    print("✅ Gold Data Loaded")
    print(f"Total Records: {df.count()}")
    df.printSchema()
    df.show(5, truncate=False)

    # -------------------------
    # 1. Split train/test
    # -------------------------
    # - Ensures reproducibility with a fixed seed.
    # - 80% for training, 20% for evaluation.
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # -------------------------
    # 2. Train Linear Regression Model
    # -------------------------
    # Fitting a Linear Regression model using feature vector and label
    lr = LinearRegression(featuresCol="features", labelCol="label")
    model = lr.fit(train_df)

    # Optional: Print model coefficients and intercept
    print("✅ Model Trained")
    print(f"Intercept: {model.intercept}")
    print(f"Coefficients: {model.coefficients}")

    # -------------------------
    # 3. Evaluate on Test Data
    # -------------------------
    # - RMSE (Root Mean Squared Error): Measures prediction error magnitude.
    # - R² (Coefficient of Determination): Measures how well features explain variance in the target.
    predictions = model.transform(test_df)
    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    print(f"✅ Model Evaluation \n RMSE: {rmse:.3f} \n R²: {r2:.3f}")

    # -------------------------
    # 4. Save Model
    # -------------------------
    model_path = os.path.join("..", "models", "linear_regression_crop_yield")
    model.write().overwrite().save(model_path)
    print(f"✅ Model saved at: {model_path}")

    spark.stop()

# Linear Regression ka matlab kya hai?
# Soch ke dekho: agar hum maan lein ki baarish, temperature, aur soil fertility jaise factors crop ka yield directly affect karte hain, toh hum ek seedha rista banate hain — jaise:
# "Zyada baarish hui toh yield badhega, kam hui toh ghat jayega."
# Yehi seedha rista Linear Regression model pakadta hai. Ye har feature ka ek weight nikalta hai, jisse pata chalta hai ki kaunsa factor kitna important hai.
# Example dekh:
# Maan lo:
# - Rainfall = 80 cm
# - Temperature = 30°C
# - Soil Fertility Index = 0.75
# Model kuch aisa predict karega:
# Yield = 0.5 × Rainfall + 0.3 × Temperature + 0.2 × SoilFertility + bias

if __name__ == "__main__":
    main()


""""
import os
from utils import get_spark
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

def main():
    spark = get_spark("CropYield-TrainModel")

    # Load Gold dataset
    ## - This dataset already includes:
# - One-hot encoded categorical features
# - Assembled feature vector (features)
# - Target column (label)

    gold_path = os.path.join("..","data", "gold", "crop_yield_v8")
    df = spark.read.parquet(gold_path)

    print("✅ Gold Data Loaded")
    df.printSchema()
    df.show(5, truncate=False)

    # -------------------
    # 1. Split train/test
    # -------------------
#     - Ensures reproducibility with a fixed seed.
#     - 80% for training, 20% for evaluation.

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # -------------------
    # 2. Train Linear Regression Model
    # -------------------
    ## Fiting a Linear Regression model using feature vector and label
    lr = LinearRegression(featuresCol="features", labelCol="label")
    model = lr.fit(train_df)

    # -------------------
    # 3. Evaluate on Test Data
    # -------------------

    # - RMSE (Root Mean Squared Error): Measures prediction error magnitude.
    # - R² (Coefficient of Determination): Measures how well features explain variance in the target.

    predictions = model.transform(test_df)

    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print(f"✅ Model Evaluation | RMSE: {rmse:.3f} | R²: {r2:.3f}")

    # -------------------
    # 4. Save Model
    # -------------------
    model_path = os.path.join("..","models", "linear_regression_crop_yield")
    model.write().overwrite().save(model_path)

    print(f"✅ Model saved at: {model_path}")
    spark.stop()

#      Linear Regression ka matlab kya hai?
#      Soch ke dekho: agar hum maan lein ki baarish, temperature, aur soil fertility jaise factors crop ka yield directly affect karte hain, toh hum ek seedha rista banate hain — jaise:
#      "Zyada baarish hui toh yield badhega, kam hui toh ghat jayega."
#       Yehi seedha rista Linear Regression model pakadta hai. Ye har feature ka ek weight nikalta hai, jisse pata chalta hai ki kaunsa factor kitna important hai.

#  Example dekh:
# Maan lo:
# - Rainfall = 80 cm
# - Temperature = 30°C
# - Soil Fertility Index = 0.75
# Model kuch aisa predict karega:
# Yield = 0.5 × Rainfall + 0.3 × Temperature + 0.2 × SoilFertility + bias
if __name__ == "__main__":
    main()
"""