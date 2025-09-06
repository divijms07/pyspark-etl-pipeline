Crop Yield Prediction Pipeline
This project is a full-stack machine learning pipeline designed to predict crop yield (in tons per hectare) using environmental and agronomic features. It includes modular ETL stages, model training, prediction, and a Streamlit-based UI for real-time inference.

Project Structure

Crop Yeild Prediction/
├── .venv/                     # Python virtual environment
├── data/
│   ├── raw/                   # Raw input data
│   ├── bronze/                # Extracted and minimally cleaned data
│   ├── silver/                # Transformed and feature-engineered data
│   ├── gold/                  # Final model-ready dataset
│   └── predictions/          # Saved model predictions
├── models/
│   └── linear_regression_crop_yield/  # Trained Spark ML model
├── logs/
│   └── logs.txt              # Pipeline and model logs
├── artifacts/                # Optional intermediate outputs
├── src/
│   ├── extract_bronze.py     # Extract raw → bronze
│   ├── transform_silver.py   # Bronze → silver transformation
│   ├── enrich_gold.py        # Silver → gold enrichment
│   ├── train_model.py        # Train and save ML model
│   ├── predict.py            # Batch prediction script
│   ├── visualize.py          # Streamlit UI for real-time prediction
│   ├── utils.py              # Spark session and helper functions
│   └── __init__.py
└── README.md                 # Project documentation

Features
- Modular ETL pipeline using Spark
- Feature engineering including nutrient indices, rainfall normalization, and derived metrics
- Linear Regression model trained using Spark MLlib
- Streamlit UI for real-time prediction
- Batch prediction support with saved .parquet outputs
- Robust logging and directory structure for auditability

Setup Instructions

git clone https://github.com/your-username/crop-yield-prediction.git
cd crop-yield-prediction

Create virtual environment

python -m venv .venv
.venv\Scripts\activate

Install dependencies

pip install -r requirements.txt

- Run ETL pipeline

python src/extract_bronze.py
python src/transform_silver.py
python src/enrich_gold.py

Train model

python src/train_model.py




