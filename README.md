# Marketing-funnel-dropoff.sql
SQL queries for visualizing marketing funnel and drop-off analysis using BigQuery and Looker Studio.

# Marketing Funnel & Drop-Off Analysis (BigQuery + GA4 + Looker Studio)

This repository includes **two optimized SQL queries** for analyzing user journey and conversion drop-offs using **BigQuery GA4 export data**, built for visualization in **Looker Studio**.

Both queries are designed to work **incrementally** to reduce cost — but require an **initial non-incremental run** to create the table structure and populate historical data.

---

## 📁 Included Queries

### 1. `01_funnel_summary.sql`
Builds a session-based funnel breakdown with steps like `first_visit`, `item_view`, `add_to_basket`, `checkout`, and `purchase`. The query creates a table that counts user sessions completing each step per landing page.

### 2. `02_dropoff_analysis.sql`
Calculates step-by-step drop-off percentages across the funnel for specified landing pages and totals purchase value per session.

---

## ⚙️ How to Use

### ✅ Step 1: Customize the Code

Before running either SQL:

- Replace `your_project.your_dataset.your_funnel_table` and `your_target_table` with your actual **BigQuery dataset and table names**
- Customize the funnel event names if different (e.g. `get_started_click`, `checkout_button_click`, etc.)
- Modify the landing page URLs in the second query to match your tracked pages
- Optionally adjust the time window (currently set to the **last 3 days**) via:
  ```sql
  DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)

### 📦 Step 2: Initial Setup (Non-Incremental)
Copy and run the first query (01_funnel_summary.sql) without the MERGE block — just the inner SELECT ... section — to create and populate the base funnel table.

Repeat the same for the second query (02_dropoff_analysis.sql) — run only the final SELECT part into a new table.

You can use:

sql
Copy
Edit
CREATE OR REPLACE TABLE your_dataset.your_table AS
SELECT ...

### 🔁 Step 3: Switch to Incremental Mode
After initial data is loaded:

Replace your code with the full version in this repo, which uses MERGE to incrementally update your tables.

Set these up as scheduled queries in BigQuery (daily or hourly).

### 📊 Step 4: Connect to Looker Studio
In Looker Studio, click “Create Data Source”

Connect your BigQuery project and select the two tables you just created

Build your funnel visualization or drop-off dashboard using metrics like:

Step counts

Drop-off %

Total purchase value

Page-level breakdowns

---

## ✅ Benefits

🔄 Incremental Processing – Reduces BigQuery cost by only querying recent GA4 tables

📉 Drop-off Insights – Understand where users leave the funnel

🔍 Page-Level Segmentation – Group funnel stats by landing page

📈 Looker Studio Ready – Use directly in visual dashboards

---

👨‍💻 Author
Ali Ahmadi
GitHub Profile
