# 🌾 Crop Yield Prediction Pipeline  

> **ETL pipeline for agricultural data**: from raw soil, weather & crop data → transformed layers → insights & visualizations.  
> Built with **PySpark**, **Pandas**, and **Seaborn/Matplotlib**.  

---

## 📌 Project Overview  

This project builds a **data pipeline** to process agricultural datasets and prepare them for **yield prediction & analysis**.  
The pipeline follows a **medallion architecture**:

- **Bronze Layer** → Raw data ingestion (CSV → Parquet & CSV)  
- **Silver Layer** → Data cleaning, unit normalization, feature engineering  
- **Gold Layer** → Final curated dataset ready for visualization & modeling  

🔑 **Key Features**:  
- Handles missing values automatically  
- Converts rainfall & temperature to consistent units  
- Engineers advanced agricultural features:
  - Rainfall deviation (per region)  
  - Soil fertility index & NPK ratios  
  - Climate stress index  
  - Agricultural support levels  
- Saves results in both **Parquet (fast)** & **CSV (demo-ready)**  
- Includes **visualization module** for insights  

---

## 🏗️ Pipeline Architecture  

```mermaid
flowchart LR
    A[Raw CSV Data] -->|Extract| B[Bronze Layer]
    B -->|Transform| C[Silver Layer]
    C -->|Enrich| D[Gold Layer]
    D -->|Visualize| E[Insights & Graphs]
```

---

## 📂 Project Structure  

```
pyspark-etl-pipeline/
│── src/
│   ├── extract_bronze.py     # Load raw CSV → Bronze (Parquet + CSV)
│   ├── transform_silver.py   # Clean + Feature Engineering → Silver
│   ├── enrich_gold.py        # Final dataset → Gold
│   ├── visualize_gold.py     # Visualization module
│   └── utils.py              # Spark session helper
│
│── data/                     # (ignored in git, huge files)
│   ├── raw/                  # Input CSV
│   ├── bronze/               # Bronze outputs
│   ├── silver/               # Silver outputs
│   └── gold/                 # Gold outputs
│
│── plots/                    # Auto-generated charts (ignored in git)
│── requirements.txt
│── README.md
│── .gitignore
```

---

## 📊 Visualizations & Insights  

✅ **Impact of Fertilizer & Irrigation**  
- Clear evidence that **support level (fertilizer + irrigation)** boosts yield.  

✅ **Yield vs Rainfall Deviation**  
- Too little or too much rainfall reduces yield.  
- Highlights importance of **water management**.  

✅ **Yield vs Climate Stress Index**  
- High stress values strongly correlate with lower yields.  

✅ **Crop vs Support Distribution**  
- Shows which crops rely most on fertilizer/irrigation.  

---

## 🚀 How to Run  

1️⃣ Clone repo & create virtual environment  
```bash
git clone https://github.com/yourusername/crop-yield-pipeline.git
cd crop-yield-pipeline
python -m venv .venv
source .venv/bin/activate   # (Linux/Mac)
.venv\Scripts\activate      # (Windows)
```

2️⃣ Install dependencies  
```bash
pip install -r requirements.txt
```

3️⃣ Run pipeline step-by-step  
```bash
cd src
python extract_bronze.py
python transform_silver.py
python enrich_gold.py
python visualize_gold.py
```

---

## 🖼️ Example Visuals  

📌 *Impact of Agricultural Support on Yield*  
![yield_by_support](plots/yield_by_support.png)  

📌 *Yield vs Climate Stress*  
![yield_vs_climate_stress](plots/yield_vs_climate_stress.png)  

📌 *Crop vs Support Distribution*  
![crop_vs_support](plots/crop_vs_support.png)  

---

## ⚙️ Tech Stack  

- **PySpark** → ETL & Feature Engineering  
- **Pandas** → Gold layer handling  
- **Seaborn & Matplotlib** → Visualization  
- **Parquet & CSV** → Storage formats  

---

## ✨ Future Scope  

- Extend pipeline with **ML model for yield prediction**  
- Add **real-time weather data ingestion**  
- Build **dashboard (Streamlit / PowerBI)** for farmer-friendly insights  





