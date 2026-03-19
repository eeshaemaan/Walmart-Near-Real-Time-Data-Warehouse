# Walmart Near-Real-Time Data Warehouse 

A complete Data Warehousing project that simulates near-real-time ETL using the Hybrid Join algorithm to integrate streaming transactional data with master data for advanced analytics.

---

## Project Overview
This project builds a near-real-time Data Warehouse for Walmart to process and analyze streaming transactional data efficiently. It simulates real-world ETL pipelines using Python and integrates data into a Star Schema for OLAP analysis.

---

## Key Objectives
- Integrate streaming transaction data with master data
- Implement Hybrid Join algorithm for efficient joins
- Design a Star Schema Data Warehouse
- Enable advanced analytical queries (OLAP)

---

## Core Concepts

- Data Warehousing
- ETL (Extract, Transform, Load)
- Stream Processing
- Star Schema Design
- OLAP Queries (Slice, Dice, Drill-down)

---

## Hybrid Join Algorithm

The Hybrid Join algorithm enables efficient joining of:
- Streaming Data (S)
- Disk-based Master Data (R)

### Key Components:
- Hash Table (10000 slots)
- FIFO Queue (for fairness)
- Disk Buffer (partition-based loading)
- Stream Buffer (handles bursty data)

### Features:
- Memory-efficient processing
- Near real-time data integration
- Thread-based stream simulation

---

## Data Warehouse Schema

### Star Schema Design

#### Fact Table:
- Fact_Sales (Revenue, Quantity, Unit Price)

#### Dimension Tables:
- Dim_Time
- Dim_Customer
- Dim_Product
- Dim_Store
- Dim_Supplier

---

## Analytical Queries

- Top revenue products (weekday vs weekend)
- Customer demographics analysis
- Sales by occupation & product category
- Revenue trends (monthly, quarterly)
- Product affinity analysis
- Sales growth & volatility
- Seasonal analysis

---

## Technologies Used

- Python (Threading, Data Structures)
- MySQL
- SQL
- Pandas

---

## How to Run

1. Run `DWBI_Proj.sql` in MySQL
2. Configure DB credentials in Python
3. Run Hybrid Join script
4. Execute OLAP queries

---

## Project Structure

- `/DWBI_Proj` → Schema creation
- `/hybrid_join` → ETL implementation
- `Project Report` → Report

---

## Limitations

- Memory constraints due to fixed hash table
- FIFO processing delays newer records
- Dependency on master data availability

---

## Learning Outcomes

- Real-time ETL pipeline design
- Efficient join algorithms
- Data warehouse modeling
- Handling streaming data & concurrency

---

## Author
Eesha Emaan

---
