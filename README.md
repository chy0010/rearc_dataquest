# 🧭 Data Engineering Quest – Complete Solution

## 📑 Table of Contents
- [Part 1: AWS S3 — Dataset Replication](#-part-1-aws-s3--dataset-replication)
- [Part 2: API Integration — U.S. Census Population](#-part-2-api-integration--us-census-population)
- [Part 3: Data Analytics — Jupyter Notebook](#-part-3-data-analytics--jupyter-notebook)
- [Part 4: Infrastructure as Code — AWS CDK](#-part-4-infrastructure-as-code--aws-cdk)
- [Documentation](#-documentation)
- [Contact](#-contact)

---

## 🚀 Part 1: AWS S3 — Dataset Replication

### ✅ Completed Tasks
- Downloaded and processed the **BLS (Bureau of Labor Statistics)** public dataset.
- Cleaned and republished all files into **Amazon S3**.
- Built a reusable Python ingestion script that:
  - Syncs new files dynamically
  - Deletes files removed from the source
  - Prevents duplicate uploads
  - Does **not** rely on hard-coded filenames

### 📦 S3 Bucket

s3://rearc-dataquest-quest/


### 📁 Files Included
- `pr.data.0.Current`
- `pr.data.1.AllData`
- `pr.class`
- `pr.contacts`
- `pr.measure`
- `pr.footnote`
- Additional dataset metadata
- `us_population.json` *(generated in Part 2)*

---

## 🌐 Part 2: API Integration — U.S. Census Population

### ✅ Completed Tasks
- Integrated with the **U.S. Census Population API**.
- Normalized and cleaned the JSON response.
- Standardized column names and removed whitespace.
- Uploaded the processed file to the same S3 bucket.

### 📄 Script

sync_population_to_s3.py


### 📁 Output

us_population.json


Stored in:
`s3://rearc-dataquest-quest/`

---

## 📊 Part 3: Data Analytics 

### 🔧 Technologies Used
- Python
- Pandas
- (Optional) PySpark
- boto3 (S3 integration)

### 📈 Analytics Performed

#### 1️⃣ Population Mean & Standard Deviation (2013–2018)
Computed:
- Mean U.S. population
- Standard deviation

#### 2️⃣ Best Year per `series_id`
From `pr.data.0.Current`:
- Aggregated quarterly values by year
- Identified the year each `series_id` performed best

#### 3️⃣ Join: Population × Time Series
Filtered:
- `series_id = PRS30006032`
- `period = Q01`

Joined with corresponding population for that year.

### 📄 Script

data_analytics.py

---

## 🏗 Part 4: Infrastructure as Code — AWS CDK

### 🔧 Architecture Overview
- S3 bucket definition
- IAM roles & policies
- (Optional) Lambda for ingestion or automation
- (Optional) EventBridge schedule
- Fully reproducible setup using AWS CDK

---

## 📘 Documentation
Additional notes and design decisions are included throughout the repository.

---


---

## 🔗 Links to Data in S3

| File Name | Public S3 URL |
|----------|---------------|
| `pr.class` | https://rearc-dataquest-quest.s3.amazonaws.com/pr.class |
| `pr.contacts` | https://rearc-dataquest-quest.s3.amazonaws.com/pr.contacts |
| `pr.data.0.Current` | https://rearc-dataquest-quest.s3.amazonaws.com/pr.data.0.Current |
| `pr.data.1.AllData` | https://rearc-dataquest-quest.s3.amazonaws.com/pr.data.1.AllData |
| `pr.duration` | https://rearc-dataquest-quest.s3.amazonaws.com/pr.duration |
| `pr.footnote` | https://rearc-dataquest-quest.s3.amazonaws.com/pr.footnote |
| `pr.measure` | https://rearc-dataquest-quest.s3.amazonaws.com/pr.measure |
| `pr.period` | https://rearc-dataquest-quest.s3.amazonaws.com/pr.period |
| `us_population.json` | https://rearc-dataquest-quest.s3.amazonaws.com/us_population.json |


## 📬 Contact
For questions or clarifications, feel free to reach out or create an issue.
