# Walmart Near-Real-Time Data Warehouse (Simulated)

A complete Data Warehousing project that simulates near-real-time ETL using the Hybrid Join algorithm to integrate transactional data streams with master data for advanced analytics.

---

## Project Overview
This project builds a simulated near-real-time Data Warehouse for Walmart to process and analyze continuously arriving transactional data. It demonstrates real-world ETL pipeline concepts using Python by ingesting data in chunks (to mimic streaming) and integrating it into a Star Schema for OLAP analysis.

---

## Key Objectives
- Simulate streaming transaction data and integrate it with master data
- Implement the Hybrid Join algorithm for efficient stream–relation joins
- Design a Star Schema Data Warehouse
- Enable advanced analytical queries (OLAP)

---

## Core Concepts

- Data Warehousing
- ETL (Extract, Transform, Load)
- Stream Processing (Simulated)
- Star Schema Design
- OLAP Queries (Slice, Dice, Drill-down)

---

## Hybrid Join Algorithm

The Hybrid Join algorithm enables efficient joining of:
- Simulated Streaming Data (S)
- Disk-based Master Data (R)

### Key Components:
- Hash Table (10000 slots)
- FIFO Queue (for fairness)
- Disk Buffer (partition-based loading)
- Stream Buffer (handles bursty data simulation)

### Features:
- Memory-efficient processing
- Simulated near-real-time data integration
- Multi-threaded stream simulation using Python

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
2. Configure database credentials in the Python script
3. Run the Hybrid Join ETL script
4. Execute OLAP queries on the populated Data Warehouse

---

## Project Structure

- `/DWBI_Proj` → Schema creation
- `/hybrid_join` → ETL implementation
- `Project Report` → Report

---

## Limitations

- Simulated streaming using CSV files instead of real-time data sources
- Memory constraints due to fixed hash table size
- FIFO processing may delay newer records under heavy load
- Dependency on availability and completeness of master data

---

## Learning Outcomes

- Understanding of near-real-time ETL concepts through simulation
- Implementation of efficient stream–relation join algorithms
- Practical experience with data warehouse modeling (Star Schema)
- Handling concurrency and streaming-like data processing in Python

---

## Author
Eesha Emaan

---
