🎮 Game Data Engineering for Trend Analysis
📌 Project Overview

This project builds an end-to-end Data Engineering & Machine Learning pipeline to analyze game market trends.
Data is collected from MongoDB, processed using Apache Spark (PySpark), stored in PostgreSQL, and analyzed with unsupervised machine learning (KMeans clustering).
The results are designed to be visualized on BI dashboards (Looker / Looker Studio).

🏗️ System Architecture
```
MongoDB
   │
   ▼
PySpark ETL
   │
   ▼
PostgreSQL (Data Warehouse)
   │
   ▼
Spark ML (KMeans Clustering)
   │
   ▼
Analytics Tables (Trends)
   │
   ▼
Looker Dashboard
bash ```

🗃️ Data Sources

MongoDB
Raw game data crawled from external platforms (e.g. Steam / Game APIs)

🧱 Data Warehouse Schema (PostgreSQL)
Core Tables
Table	Description
games	Game basic information (name, tags, release date, description)
game_details	Pricing and review information
game_companies	Developer & publisher mapping
system_requirements	Minimum hardware requirements
ML & Analytics Tables
Table	Description
ml_game_clusters	Output of KMeans clustering
game_trends	Business-mapped trend labels
🔄 ETL Process
1️⃣ Extract

Read raw game data from MongoDB using PySpark Mongo Connector

2️⃣ Transform

Data cleaning and normalization

Schema splitting:

Game metadata

Pricing & reviews

Companies

System requirements

3️⃣ Load

Store structured data into PostgreSQL

Indexing for analytics performance

🤖 Machine Learning Pipeline
Model

Algorithm: KMeans

Type: Unsupervised Learning

Features Used

final_price_number_original

final_price_number_discount

review_score (derived from review summary)

Pipeline Steps

Feature assembling

Feature scaling (StandardScaler)

KMeans clustering

Persist results into PostgreSQL

📊 Business Trend Mapping

Clusters are translated into business-friendly trend labels:

Cluster	Trend Label
0	Popular & Cheap
1	Premium / Hardcore
2	Sale Driven
Others	Other

These labels enable non-technical stakeholders to understand market trends.

📈 Analytics Use Cases

Game trend distribution by genre

Trend evolution over release years

Price vs popularity analysis

Publisher-based trend comparison

📊 BI & Visualization

Looker / Looker Studio

Data Source: PostgreSQL

KPI Examples:

Number of games per trend

Trend distribution by genre

Price segmentation insights

🛠️ Tech Stack
Layer	Technology
Data Ingestion	MongoDB
Processing	PySpark
Storage	PostgreSQL
ML	Spark MLlib
BI	Looker
Language	Python
Environment	macOS (M2), Apache Spark
🚀 How to Run
Run ML Clustering
spark-submit \
  --packages org.postgresql:postgresql:42.7.4 \
  ML/game_clustering.py

📂 Project Structure
Game-Data-Engineering-for-Trend-Analysis/
│
├── ETL-Process/
│   ├── extract/
│   ├── transform/
│   ├── load/
│
├── ML/
│   └── game_clustering.py
│
├── sql/
│   ├── schema.sql
│   ├── views.sql
│
└── README.md

🎯 Key Learnings

Building scalable ETL pipelines with PySpark

Designing analytical schemas in PostgreSQL

Applying unsupervised ML for business insights

Integrating ML outputs into BI dashboards

👤 Author

Trần Hoài Lộc Lê
Data Engineer | Analytics Engineer
📧 (optional email)
🔗 GitHub / LinkedIn