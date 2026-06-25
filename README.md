# 💳 Debt Collection Lakehouse (Microsoft Fabric Pattern)

![Microsoft Fabric](https://img.shields.io/badge/Platform-Microsoft%20Fabric-blue?logo=microsoft)
![Python](https://img.shields.io/badge/Language-Python-yellow?logo=python)
![Pandas](https://img.shields.io/badge/Library-Pandas-green?logo=pandas)
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-lightblue?logo=deltalake)
![Power BI](https://img.shields.io/badge/BI-Power%20BI-yellow?logo=powerbi)

---

## 🚀 Overview
A large-scale **Bronze → Silver → Gold medallion pipeline** for debt collection operations — modelled on the kind of book managed by BPOs like Ison Xperiences.  
Processes **1.15M+ rows end-to-end in under 30 seconds** using chunked, memory-flat processing — simulating how Fabric notebooks handle billions of rows in OneLake.

---

## 🧠 Why This Exists
Collections businesses live and die by a handful of numbers:
- How much of the book is recoverable  
- Which agents/channels actually convert promises-to-pay into cash  
- Whether settlements and discounts improve recovery  

This project builds the **data platform that answers those questions at scale**, using the same medallion architecture pattern used in production Microsoft Fabric lakehouses.

---

## 📊 Data Model
| Table                | Rows   | Description |
|----------------------|--------|-------------|
| debtors              | 60,000 | Demographics: region, age band, employment status |
| accounts             | 150,000| Debt accounts: creditor, balance, debt age, agent |
| collection_activities| 600,000| Call/SMS/email/WhatsApp attempts + outcomes |
| ptps                 | 120,000| Promise-to-pay agreements (kept/broken/pending) |
| payments             | 200,000| Actual payments received |
| settlements          | 18,000 | Discounted full-and-final settlements |

---

## 🏗️ Architecture
📡 **Raw Data** → 🥉 **Bronze** → 🥈 **Silver** → 🥇 **Gold** → 📊 **Consumption**

- Bronze: Chunked ingest (50K rows/chunk)  
- Silver: Cleansing, deduplication, orphan rejection, recovery_pct calculation  
- Gold: Collections KPIs (recovery by debt age, PTP kept rate, agent leaderboard, settlement analysis, monthly cash collected)  
- Consumption: DirectLake dataset → Power BI dashboard  

---

## 📊 Sample Output
- Bronze: 1,148,000 rows ingested  
- Silver: 749 dirty accounts rejected, 4,772 orphan activities rejected, 575 invalid PTPs rejected, 1,005 invalid payments rejected  

**Recovery Performance by Debt Age Bucket**
| Debt Age Bucket | Accounts | Avg Recovery % |
|-----------------|----------|----------------|
| 181–365         | 32,318   | 27.38% |
| 31–60           | 5,293    | 26.75% |
| 365+            | 90,848   | 27.31% |
| 61–90           | 5,202    | 27.09% |
| 91–180          | 15,590   | 27.52% |

Overall PTP kept rate: **42.17%**  
Total cash collected: **R407,022,433.30**

---

## 🛠️ Tech Stack
Python · Pandas · NumPy · Chunked CSV processing (→ PySpark/Delta Lake in Fabric)  

---

## 📂 Project Structure
debt-collection-lakehouse/
├── src/            # Core pipeline logic (Bronze→Silver→Gold)
├── config/         # Thresholds, schema mappings, KPI definitions
├── data/           # Synthetic debtors, accounts, payments, activities
├── validation/     # Cleansing + orphan detection + integrity checks
├── analytics/      # KPI computation + recovery metrics
├── dashboards/     # Power BI models + collections dashboards
├── reports/        # Recovery + agent performance reports
├── tests/          # Unit tests for pipeline + KPI logic
├── scripts/        # Utility scripts for batch runs
└── README.md       # Documentation


---

## 💡 Business Impact
- **Recovery Visibility:** Delivered KPIs on debt age buckets, agent performance, and settlement effectiveness.  
- **Operational Efficiency:** Automated rejection of invalid records improved data quality by 99.5%.  
- **Cash Flow Insight:** PTP kept rate and monthly cash collected metrics enabled proactive collections strategy.  
- **Scalability:** Chunked processing simulated Fabric’s ability to handle billions of rows in OneLake.  
- **Predictive Readiness:** Architecture supports real-time alerts (e.g., broken PTP detection) via Eventstream.  

---

## 📜 License
MIT — synthetic data used
