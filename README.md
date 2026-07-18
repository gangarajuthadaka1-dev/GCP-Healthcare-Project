# GCP-Healthcare-Project
End-to-end healthcare claims ETL pipeline on Google Cloud Platform using PySpark, Dataproc, Cloud Storage, BigQuery, and automated data quality validation.

---

## Project Overview

This project focuses on building a data lake in Google Cloud Platform (GCP) for Revenue Cycle Management (RCM) in the healthcare domain. The goal is to centralize, clean, and transform data from multiple sources, enabling healthcare providers and insurance companies to streamline billing, claims processing, and revenue tracking.

---

## GCP Services Used

This project leverages multiple GCP services to build an efficient and scalable RCM Data Lake:

- Google Cloud Storage (GCS): Stores raw and processed data files.
- BigQuery: Serves as the analytical engine for storing and querying structured data.
- Dataproc: Used for large-scale data processing with Apache Spark.
- Cloud Composer (Apache Airflow): Automates ETL pipelines and workflow orchestration.
- Cloud SQL (MySQL): Stores transactional Electronic Medical Records (EMR) data.
- GitHub & Cloud Build: Enables version control and CI/CD implementation.
- CI/CD (Continuous Integration & Continuous Deployment): Automates deployment pipelines for data processing and ETL workflows.

---

## Key Techniques Involved

This project follows best practices to ensure scalability, reliability, and efficiency.

### 1. Metadata-Driven Approach

- Uses configs to dynamically configure ETL pipelines instead of hardcoded logic.
- Reduces manual intervention and increases automation.

### 2. Slowly Changing Dimensions (SCD) Type 2 Implementation

- Tracks historical changes in dimension tables (e.g., patient details changes, transaction information updates).
- Maintains data history by adding new records instead of overwriting existing data.

### 3. Common Data Model (CDM)

- Standardizes data across multiple hospitals to maintain consistency.
- Maps raw data into a unified schema for easier analytics and interoperability.

### 4. Medallion Architecture (Bronze → Silver → Gold Layers)

- **Bronze Layer:** Stores raw, unprocessed data.
- **Silver Layer:** Cleanses, transforms, and enriches data.
- **Gold Layer:** Stores aggregated and business-ready data for reporting.

### 5. Logging and Monitoring

- Tracks pipeline execution, errors, and performance metrics.
- Enables real-time alerts for failures or anomalies.

### 6. Error Handling

- Implements robust mechanisms to capture, log, and resolve errors during ingestion and transformation.
- Ensures data quality and prevents pipeline failures.

### 7. CI/CD Implementation

- Automates deployment and version control using GitHub + Cloud Build.
- Ensures smooth updates to data pipelines without downtime.

### 8. Other Best Practices

- Data Validation: Checks for missing values, incorrect formats, and duplicates.
- Access Controls: Implements IAM roles and permissions to secure sensitive data.
- Data Retention & Compliance: Adheres to HIPAA and other healthcare regulations.

---

## What is RCM?

Revenue Cycle Management (RCM) is the financial process that healthcare providers use to track patient care episodes from registration and appointment scheduling to the final payment of a balance. This ensures smooth operations by managing patient details, billing, insurance claims, and payments efficiently.

---

## RCM Process Breakdown

### Patient Visit Initiation

- Patient details and insurance information are collected.
- Identifies who will be responsible for payment: insurance, patient, or both.

### Service Provision

- Includes daily checkups, treatments, surgeries, and other medical services.

### Billing Generation

- The hospital generates an itemized bill for the services provided.

### Claims Review

- Insurance companies review the bill.
- They may accept the claim fully, pay partially, or reject it.

### Payments and Follow-ups

- If only a partial payment is covered by insurance, the patient may need to pay the remaining amount.
- Hospitals follow up on outstanding payments.

### Tracking and Improvement

- Ensures financial sustainability while maintaining quality care.

---

## Role of Data Engineering in RCM

As data engineers, we manage data from various sources and create ETL pipelines that transform raw data into structured tables in the data lake for reporting and dashboard generation.

- Data Ingestion
- Data Storage and Management
- Data Processing
- Data Analysis
- Data Pipelines
- Data Orchestration
- Data Visualization

---

## Data Sources

### 1. EMR (Electronic Medical Records) Data Source - Cloud SQL Database

Example scenario: Two hospitals with separate databases:

- Hospital 1 → `supreme_hosp1_db`
- Hospital 2 → `supreme_hosp2_db`

Each hospital stores data related to:

- Patients
- Providers
- Departments
- Transactions
- Encounters

### 2. Claims Data Source

- Comes from insurance companies.
- Provided as flat files.
- Stored in a designated folder in the data lake (Landing Zone) on a monthly basis.

### 3. CPT (Current Procedural Terminology) Codes

- Public API providing a standardized system to describe medical, surgical, and diagnostic procedures.

### 4. NPI (National Provider Identifier) Data

- Public API providing unique identifiers for healthcare providers.

### 5. ICD (International Classification of Diseases) Codes

- Public API providing standardized disease codes and descriptions.

---

## Architecture
<img width="1934" height="813" alt="Image" src="https://github.com/user-attachments/assets/3dba9e93-2c49-4676-bdfa-fda457e37c3c" />

---
## Data Engineering Workflow

### 1. Extract Data from Sources

- PySpark jobs to extract EMR data from two hospital databases.
- Flat file ingestion for claims data.
- API calls for CPT, NPI, and ICD data.

### 2. Transform Data

- Clean and standardize data.
- Convert raw records into structured fact and dimension tables.

### 3. Load Data into BigQuery

- Store fact and dimension tables for analytics and reporting.

### 4. Orchestration

- Orchestrate all jobs based on dependencies and execution order.

### 5. Enable Reporting & KPIs

- Provide data for dashboards and reports.
- Help stakeholders analyze revenue performance and optimize processes.

### 6. Expected Outcomes

- Efficient Data Pipeline: Automating the ingestion and transformation of RCM data.
- Structured Data Warehouse: Gold tables in BigQuery for analytical queries.
- KPI Dashboards: Insights into revenue collection, claims processing efficiency, and financial trends.

---

This project will equip with real-world data engineering skills while understanding the critical components of RCM in healthcare.
