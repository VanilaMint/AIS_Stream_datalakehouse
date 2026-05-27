# 🚢 Real-Time AIS Stream Data Lakehouse
![trip select option and kpis](./img/image.png)
![ship movements on the map](./img/image-1.png)
![livedashboard](./img/livedashboard-zoom.png)

## Executive Summary
This project is an end-to-end data engineering pipeline that processes live Automatic Identification System (AIS) ship tracking data. It ingests continuous, real-time data streams, cleans and structures the information, and routes it into a scalable Data Lakehouse. The final structured data powers live, interactive dashboards to monitor global ship movements and historical trip analytics.

## The Architecture & Data Flow
The pipeline is built to handle high-throughput streaming data efficiently, breaking down the journey into distinct stages:

* **1. Ingestion (Python & Kafka):** A custom producer script connects to a live AIS feed and streams the raw ship telemetry (position reports, ship metadata, basestation locations) directly into an Apache Kafka message broker.

* **2. Stream Processing (Apache Flink & PostgreSQL):** Apache Flink consumes the Kafka streams in real-time. It separates the data, cleans it, and enriches it using static ship metadata stored in a PostgreSQL database.

* **3. The Lakehouse (Apache Iceberg):** Processed streams are ingested directly into Apache Iceberg tables, allowing the data to be used for analytical purposes

* **4. Data Modeling (dbt):** Using Data Build Tool (dbt), the raw Iceberg data is transformed into a clean star schema. This process automatically groups thousands of individual, continuous pings into logical, distinct "ship trips" for easier analysis.

* **5. Visualization (Grafana):** The structured data is connected to Grafana, providing an accessible, visual interface to track live ships and review historical trip data.

## Tech Stack
* **Languages:** Python, SQL
* **Streaming & Processing:** Apache Kafka, Apache Flink
* **Storage & Architecture:** Apache Iceberg, PostgreSQL
* **Transformation:** dbt (Data Build Tool)
* **Infrastructure & Visualization:** Docker, Grafana, Redis

## Key Features
* **Continuous Real-Time Processing:** Handles live, non-stop data streams using Apache Flink, utilizing event-time joins to efficiently manage memory and prevent infinite state growth during continuous operations.
* **Analytics-Ready Dimensional Modeling:** Transforms chaotic, raw telemetry streams into a clean, highly queryable Star Schema (Facts and Dimensions) using dbt
* **SCD Type 2 Dimension Tracking:** Implements Slowly Changing Dimension (SCD) Type 2 architecture to accurately track and preserve historical changes in ship static metadata over time, ensuring a reliable audit trail.
* **Optimized Incremental Builds & Watermarking:** Uses custom watermarking logic to handle late-arriving data and leverages dbt's incremental materializations (merge/append) to avoid expensive full-table rebuilds and optimize processing time.
* **Containerized Environment:** The entire infrastructure (Kafka, Flink, databases) is managed via Docker Compose for easy deployment and teardown.
## How to Run Locally

WIP