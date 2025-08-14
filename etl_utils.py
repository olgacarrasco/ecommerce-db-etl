import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging

logging.basicConfig(level=logging.INFO)

def load_csv_data():
    """Load CSV files from the same folder as this script."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    logging.info(f"Loading CSV files from: {base_path}")
    
    customers_df = pd.read_csv(os.path.join(base_path, "customers.csv"))
    products_df = pd.read_csv(os.path.join(base_path, "products.csv"))
    orders_df = pd.read_csv(os.path.join(base_path, "orders.csv"))
    
    return customers_df, products_df, orders_df

def clean_data(customers_df, products_df, orders_df):
    """Drop rows with missing values."""
    customers_df.dropna(inplace=True)
    products_df.dropna(inplace=True)
    orders_df.dropna(inplace=True)
    return customers_df, products_df, orders_df

def transform_orders(orders_df, customers_df, products_df):
    """Merge orders with customers and products."""
    merged_df = (
        orders_df.merge(customers_df, left_on='customer_id', right_on='id', how='left')
                 .merge(products_df, left_on='product_id', right_on='id', how='left')
    )
    merged_df['order_date'] = pd.to_datetime(merged_df['order_date'])
    
    # Drop redundant ID columns
    merged_df.drop(columns=['id_x', 'id_y'], inplace=True, errors='ignore')
    return merged_df

def connect_db():
    """Connect to database using environment variables."""
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", 5432)
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_name]):
        raise ValueError("Database credentials are not fully set in environment variables.")

    logging.info(f"Connecting to DB at {db_host}:{db_port}/{db_name} with user {db_user}")
    
    db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require'
    engine = create_engine(db_url)
    return engine

def load_to_db(engine, customers_df, products_df, merged_df, replace=False):
    """
    Load dataframes into the database.
    
    If replace=True, existing tables are dropped.
    Otherwise, data is appended.
    """
    if replace:
        with engine.connect() as conn:
            logging.info("Dropping tables with CASCADE...")
            conn.execute(text("DROP TABLE IF EXISTS orders CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS customers CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS products CASCADE;"))
            logging.info("Tables dropped.")
    
    # Load data
    customers_df.to_sql('customers', engine, if_exists='append', index=False)
    products_df.to_sql('products', engine, if_exists='append', index=False)
    merged_df.to_sql('orders', engine, if_exists='append', index=False)
    logging.info("Data loaded into database.")

def run_etl(replace=False):
    """Full ETL pipeline."""
    customers_df, products_df, orders_df = load_csv_data()
    customers_df, products_df, orders_df = clean_data(customers_df, products_df, orders_df)
    
    # Strip spaces from column headers
    customers_df.columns = customers_df.columns.str.strip()
    products_df.columns = products_df.columns.str.strip()
    orders_df.columns = orders_df.columns.str.strip()
    
    merged_df = transform_orders(orders_df, customers_df, products_df)
    
    engine = connect_db()
    load_to_db(engine, customers_df, products_df, merged_df, replace=replace)
