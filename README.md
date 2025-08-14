# Overview
This project automates an ETL (Extract, Transform, Load) pipeline for an e-commerce database using **Apache Airflow**.  
It extracts data from CSV files, APIs, or external databases, transforms it to match the `ecommerce-db` schema, and loads it into a **Supabase PostgreSQL** instance.  

The workflow supports scheduling, error handling, and logging to ensure reliable and automated data updates.

---

## Features
- Extract data from CSV, APIs, or other databases  
- Transform raw data according to business rules  
- Load cleaned data into Supabase PostgreSQL tables  
- Automate execution via Airflow DAGs  
- Monitor logs and handle errors efficiently  

---

## Database
- **Type:** Supabase PostgreSQL  
- **Instance:** `ecommerce-db`  
- **Tables:**  
  - `customers`  
  - `products`  
  - `orders`  

---

## Technologies
- Python (**pandas**, **psycopg2**)  
- Apache Airflow  
- Supabase client libraries / PostgreSQL  
- Jupyter Notebook (for demo & testing)  

---

## Project Structure
ecommerce-db-etl/

- **ecommerce-db-etl/**
  - **dags/**
    - `etl_dag.py` – Airflow DAG for automated ETL
  - **etl/**
    - `etl_utils.py` – ETL utility functions
  - **notebooks/**
    - `etl_demo.ipynb` – Demo notebook for testing
  - **data/**
    - `customers.csv` – Sample CSV data
    - `products.csv`
    - `orders.csv`
  - `config_example.py` – Example config for DB credentials
  - `requirements.txt` – Python dependencies
  - `README.md` – Project documentation

---

## How to Run

1. **Set up configuration**  
   - Copy `config_example.py` to `config.py`  
   - Fill in your real Supabase credentials (**never commit them**).  

2. **Prepare data**  
   - Place your CSV files in the `data/` folder.  

3. **Start Apache Airflow**  
   - Add `etl_dag.py` to your Airflow DAGs folder.  
   - Trigger `csv_to_postgres_etl` DAG manually or via schedule.  

4. **Monitor execution**  
   - View logs in the Airflow UI to confirm success.  

---

## Workflow Steps
1. Connect to Supabase `ecommerce-db` using API credentials.  
2. Extract data from external sources.  
3. Clean and transform data according to business rules.  
4. Load data into Supabase tables via API or SQL.  
5. Automate and schedule ETL execution.  
6. Monitor logs and handle errors.  

---

## License
This project is licensed under the **MIT License**.
