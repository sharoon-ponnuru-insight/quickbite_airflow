# QuickBite Data Platform · Automated Food Delivery Pipeline

An end-to-end, automated, and idempotent data engineering platform built for **QuickBite**, a food delivery business. This platform orchestrates daily operational file ingestion from Google Drive into Cloud Storage landing zones, stages raw CSVs in Google BigQuery, and transforms them through a **Medallion Lakehouse Architecture** (Bronze → Silver → Gold) using **Google Cloud Dataform in Workspace Mode (Git-less setup)**.

---

## 1. System Architecture & Data Flow

```
graph LR
    subgraph INPUT ["1. INPUT LAYER"]
        GDrive["Google Drive<br/>(Daily CSV Exports)"]
    end

    subgraph ORCHESTRATION ["2. ORCHESTRATION & INGESTION (Cloud Composer / Airflow - us-central1)"]
        Sensor["GoogleDriveFileExistenceSensor<br/>(Check File Presence)"]
        GCS_Op["GoogleDriveToGCSOperator<br/>(Extract to Landing)"]
        BQ_Temp["GCSToBigQueryOperator<br/>(Load to Temp Tables)"]
        BQ_Merge["BigQueryInsertJobOperator<br/>(Merge & Add Audit Fields)"]
    end

    subgraph TRANSFORMATION ["3. TRANSFORMATION LAYER (GCP Dataform - us-central1)"]
        subgraph BRONZE ["Bronze Layer"]
            Raw_Tables[("sharon_raw<br/>raw_restaurants<br/>raw_customers<br/>raw_orders<br/>raw_order_items")]
        end

        subgraph SILVER ["Silver Layer"]
            Staging["Staging Views<br/>(stg_*)"]
            Core_Dims["Core Dimensions<br/>(dim_restaurants, dim_customers)"]
            Core_Facts["Core Facts<br/>(fct_orders, fct_order_items)"]
            Quarantine[("rejected_orders<br/>(Quarantine Table)")]
        end

        subgraph GOLD ["Gold Layer"]
            Marts[("sharon_marts<br/>mart_daily_city_revenue<br/>mart_restaurant_performance<br/>mart_customer_summary")]
        end
    end

    subgraph ARCHIVING ["4. ARCHIVING & OUTPUT"]
        GCS_Landing["GCS Landing Bucket<br/>(capstone/sharon/landing/)"]
        GCS_Archive["GCSToGCSOperator<br/>(capstone/sharon/archive/)"]
        Looker["Analytics / BigQuery Console<br/>(Executive Reporting)"]
    end

    %% Relationships & Flow
    GDrive --> Sensor
    Sensor --> GCS_Op
    GCS_Op --> GCS_Landing
    GCS_Landing --> BQ_Temp
    BQ_Temp --> BQ_Merge
    BQ_Merge --> Raw_Tables
    
    Raw_Tables --> Staging
    Staging --> Core_Dims
    Staging --> Core_Facts
    Staging --> Quarantine
    
    Core_Dims --> Marts
    Core_Facts --> Marts
    
    Marts --> Looker
    Marts --> GCS_Archive
    GCS_Landing -. Move Files .-> GCS_Archive

    %% Styling with explicit black text font
    classDef inputStyle fill:#e1f5fe,stroke:#0288d1,stroke-width:2px,color:#000000;
    classDef airflowStyle fill:#fff3e0,stroke:#f57c00,stroke-width:2px,color:#000000;
    classDef bqStyle fill:#e8f5e9,stroke:#388e3c,stroke-width:2px,color:#000000;
    classDef martStyle fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000000;

    class GDrive inputStyle;
    class Sensor,GCS_Op,BQ_Temp,BQ_Merge airflowStyle;
    class Raw_Tables,Staging,Core_Dims,Core_Facts,Quarantine bqStyle;
    class Marts,Looker,GCS_Landing,GCS_Archive martStyle;

## 2. Workspace Mode Setup (Git-Less Execution)

To run Dataform without an external GitHub/Git enterprise connection:

1. **GCP Dataform Workspace Creation:**
   * Open **GCP Console → Dataform** in **`us-central1`**.
   * Create repository: `sharon-quickbite`.
   * Under **WORKSPACES**, create a development workspace: `sharon_workspace`.

2. **GCP Dataform Service Account Configuration:**
   * Open **Dataform → `sharon-quickbite` → SETTINGS**.
   * Set Service Account to: `sada-seed-2026-training@sada-seed-2025-sandbox.iam.gserviceaccount.com`.

3. **Airflow Workspace Invocation:**
   * Airflow triggers Dataform by referencing the workspace resource path directly:
     `projects/sada-seed-2025-sandbox/locations/us-central1/repositories/sharon-quickbite/workspaces/sharon_workspace`.

---

## 3. Dataform Repository Structure (`sharon_workspace`)

```
sharon_workspace/
├── workflow_settings.yaml
└── definitions/
    ├── sources/
    │   └── sources.js
    ├── staging/
    │   ├── stg_restaurants.sqlx
    │   ├── stg_customers.sqlx
    │   ├── stg_orders.sqlx
    │   └── stg_order_items.sqlx
    ├── core/
    │   ├── dim_restaurants.sqlx
    │   ├── dim_customers.sqlx
    │   ├── fct_orders.sqlx
    │   ├── fct_order_items.sqlx
    │   └── rejected_orders.sqlx
    └── marts/
        ├── mart_daily_city_revenue.sqlx
        ├── mart_restaurant_performance.sqlx
        └── mart_customer_summary.sqlx
```

---

## 4. Step-by-Step Deployment Guide

### Step 1: Initialize BigQuery Datasets in `us-central1`

Run this script in the BigQuery Console Query Editor:

```sql
CREATE SCHEMA IF NOT EXISTS `sada-seed-2025-sandbox.sharon_raw` OPTIONS(location="us-central1");
CREATE SCHEMA IF NOT EXISTS `sada-seed-2025-sandbox.sharon_core` OPTIONS(location="us-central1");
CREATE SCHEMA IF NOT EXISTS `sada-seed-2025-sandbox.sharon_marts` OPTIONS(location="us-central1");
```

### Step 2: Configure Airflow Variables

In Airflow UI (**Admin → Variables**), create the following keys:

| Variable Key | Required Value |
| :--- | :--- |
| `sharon_project_id` | `sada-seed-2025-sandbox` |
| `sharon_raw_dataset` | `sharon_raw` |
| `sharon_core_dataset` | `sharon_core` |
| `sharon_marts_dataset` | `sharon_marts` |
| `sharon_gcs_bucket` | `quickbite-sharon-bucket` *(or your GCS bucket name without `gs://`)* |
| `sharon_drive_folder_id` | `1vKmH94jDiqGuVntDSN9Si0ahBCC8bd9h` |
| `sharon_dataform_region` | `us-central1` |
| `sharon_dataform_repo` | `sharon-quickbite` |
| `sharon_dataform_workspace` | `sharon_workspace` |
| `sharon_service_account` | `sada-seed-2026-training@sada-seed-2025-sandbox.iam.gserviceaccount.com` |

### Step 3: Deploy DAG File

Upload `sharon_quickbite.py` to the `dags/` folder of your Cloud Composer storage bucket.

### Step 4: Execute & Backfill

1. Open **Airflow UI → Grid View**.
2. Click the top-level run for `2026-10-10` and click **Clear** (with *Downstream* and *Recursive* selected).
3. The DAG will run sequentially through `2026-10-10`, `2026-10-11`, `2026-10-12`, and `2026-10-13`.

---

## 5. Business Rules Compliance Matrix

| Rule # | Business Requirement | Technical Implementation |
| :-: | :--- | :--- |
| **1** | **Latest `updated_at` Wins** | Applied `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, _ingested_at DESC)` in `fct_orders.sqlx` and `dim_customers.sqlx`. |
| **2** | **Standardized Order Statuses** | Normalizes status strings to strictly `placed`, `preparing`, `out_for_delivery`, `delivered`, or `cancelled`. |
| **3** | **Latest Restaurant Snapshot & Past History** | Deduplicates `dim_restaurants` to the latest state while preserving all historical order references via `INNER JOIN` in `fct_orders.sqlx`. |
| **4** | **Quarantine Invalid Orders** | Isolates orders with unmapped `restaurant_id` or missing `customer_id` into `sharon_core.rejected_orders` with reason codes. |
| **5** | **Exclude Invalid Line Items** | Filters out items where `quantity <= 0` or `unit_price <= 0` in `stg_order_items.sqlx`. Unique key: `order_id + line_no`. |
| **6** | **Delivered Revenue Calculation** | Calculates line item revenue as `quantity * unit_price` and filters aggregates strictly on `status = 'delivered'`. |
| **7** | **Order Date Extraction** | Extracts `order_date = DATE(SAFE_CAST(order_ts AS TIMESTAMP))` without timezone manipulation. |
| **8** | **Missing Payment Method** | Defaults blank/null payment values to `'unknown'` without dropping valid orders. |

---

## 6. Data Quality Anomaly Log

| Source File | Quality Anomaly Identified | Handling Strategy Implemented |
| :--- | :--- | :--- |
| **`orders_*.csv`** | Whitespace padding in IDs and statuses (e.g. `' delivered '`) | Applied `TRIM()` and `LOWER()` across string attributes in `stg_orders.sqlx`. |
| **`customers_*.csv`** | Inconsistent string casing in email addresses and city names | Applied `LOWER()` on emails and `INITCAP()` on city names across staging views. |
| **`customers_*.csv`** | Malformed date/timestamp strings | Used `SAFE_CAST(updated_at AS TIMESTAMP)` to handle invalid formats gracefully without pipeline crashes. |
| **`order_items_*.csv`** | Negative or zero quantities and prices (`quantity = -1`) | Filtered out in `stg_order_items.sqlx` (`WHERE quantity > 0 AND unit_price > 0`). |
| **`orders_*.csv`** | Missing `customer_id` or unmapped `restaurant_id` | Quarantined into `sharon_core.rejected_orders`; excluded from gold reporting marts. |

---

## 7. BigQuery Executive Analytics Queries

### Question 1: Daily Revenue per City
```sql
SELECT 
  order_date,
  city,
  delivered_orders,
  gross_revenue
FROM `sada-seed-2025-sandbox.sharon_marts.mart_daily_city_revenue`
ORDER BY order_date DESC, gross_revenue DESC;
```

### Question 2: Restaurant Performance & Cancellation Rates
```sql
SELECT 
  restaurant_id,
  restaurant_name,
  city,
  is_active,
  total_orders,
  delivered_orders,
  cancelled_orders,
  cancellation_rate_pct,
  gross_revenue
FROM `sada-seed-2025-sandbox.sharon_marts.mart_restaurant_performance`
ORDER BY gross_revenue DESC;
```

### Question 3: Repeat Customer Summary & Spend
```sql
SELECT 
  customer_id,
  current_city,
  delivered_orders,
  total_spend,
  first_order_date,
  last_order_date,
  is_repeat_customer
FROM `sada-seed-2025-sandbox.sharon_marts.mart_customer_summary`
WHERE is_repeat_customer = TRUE
ORDER BY total_spend DESC;
```

### Data Quality Verification Check (Zero Order Leaks)
```sql
SELECT COUNT(*) AS invalid_order_id_leaks
FROM `sada-seed-2025-sandbox.sharon_core.fct_orders` f
JOIN `sada-seed-2025-sandbox.sharon_core.rejected_orders` r 
  ON f.order_id = r.order_id;
```
*(Expected Result: `0`)*