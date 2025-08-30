# Real-Time Retail Analytics Pipeline

This project demonstrates a real-time data engineering pipeline using modern data lakehouse technologies to simulate and analyze retail point-of-sale transactions across multiple store locations.

---

## Use Case Overview

A retail company with multiple physical stores wants to:

- Monitor **real-time transactions** to analyze sales trends.
- Generate **daily sales summaries** for dashboards.
- Enable **ad hoc querying and BI** access using Athena and Glue.
- Maintain a **robust, scalable, lakehouse architecture** using open standards.

This project simulates such a scenario, creating a robust streaming and batch processing pipeline using Apache Kafka, Apache Spark, Apache Iceberg, and AWS services.

---

## Architecture Overview

```mermaid
graph TD
    A[Simulated POS Transactions] --> B(Kafka Topic: pos_transactions)
    B --> C[Spark Structured Streaming Job]
    C --> D[Iceberg Raw Table on S3]
    D --> E[Spark Batch Job: Daily Sales Mart]
    E --> F[Iceberg Mart Table on S3]
    F --> G[Athena + Glue Catalog]
    G --> H[BI Tools (Looker Studio, QuickSight)]
    C -->|Scheduled| I[Dagster Job: Streaming & Batch]
```

---

## Technologies Used

| Tool                           | Purpose                                     |
| ------------------------------ | ------------------------------------------- |
| **Kafka**                      | Ingest real-time retail events              |
| **Spark Structured Streaming** | Stream processing of Kafka events           |
| **Apache Iceberg**             | ACID-compliant storage and lakehouse tables |
| **AWS S3**                     | Data lake object storage                    |
| **AWS Glue**                   | Metadata catalog (for Athena + BI tools)    |
| **Dagster**                    | Orchestration of Spark jobs                 |
| **Athena**                     | SQL querying over Iceberg tables            |

---

## Pipeline Walkthrough

### 1. **Simulating Transaction Events**

- Python scripts generate fake POS transactions for multiple stores.
- Events are sent to a Kafka topic `pos_transactions`.

### 2. **Kafka Streaming with Spark**

- Spark Structured Streaming job reads messages from Kafka.
- Transforms and writes data to Iceberg table:
  - `glue_catalog.raw.pos_transactions`
  - Partitioned by `event_time`

### 3. **Batch Aggregation**

- Spark batch job groups transactions by `store_id`, `product`, and `sale_date`
- Outputs:
  - `glue_catalog.marts.daily_sales_summary`
  - Partitioned by `sale_date`

### 4. **Orchestration with Dagster**

- Dagster manages the scheduling of:
  - Batch job: daily run
  - Spark job submission via `subprocess`
- Logs and monitoring available via Dagster UI

### 5. **Glue & Athena Integration**

- Iceberg tables are registered in AWS Glue using the `GlueCatalog`
- Athena allows ad hoc SQL querying and dashboard integration

---

## Sample Queries

```sql
-- Get daily revenue by store
SELECT store_id, sale_date, SUM(total_sales) as revenue
FROM marts.daily_sales_summary
GROUP BY store_id, sale_date
ORDER BY sale_date DESC;

-- Inspect recent raw events
SELECT * FROM raw.pos_transactions
WHERE event_time > current_date - interval '1' day;
```

---

## Directory Structure

```
retail-analytics-pipeline/
├── dags/                       # Dagster jobs and schedules
├── spark_jobs/
│   ├── streaming/              # Streaming job (Kafka to Iceberg)
│   └── batch/                  # Batch aggregation (raw to mart)
├── kafka/                      # Kafka docker-compose + simulators
├── terraform/                  # Infra-as-code (future step)
├── dagster_project/            # Dagster scaffold
├── notebooks/                  # Exploration or debugging notebooks
├── requirements.txt
└── README.md
```

---

## Iceberg Tables

| Table                       | Description           | Partitioned By     |
| --------------------------- | --------------------- | ------------------ |
| `raw.pos_transactions`      | Raw events from Kafka | `event_time (day)` |
| `marts.daily_sales_summary` | Daily aggregates      | `sale_date`        |

---

## Next Steps

1. Orchestrate with Dagster
2. Register tables in Glue + query in Athena
3. Build dashboard (Looker Studio or QuickSight)
4. Add inventory/stock mart
5. Automate full deployment using Terraform

---

## Demo Highlights

- Real-time ingestion
- Lakehouse on S3 with Iceberg
- Scalable batch processing
- Queryable in Athena
- Orchestrated pipelines

---
