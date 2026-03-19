import pandas as pd
import threading
import queue
import time
from collections import deque
import random
import os

# IMPORT FOR MYSQL 
import mysql.connector

# --- MYSQL DATABASE CONFIGURATION (Update These!) ---
db_configuration = {
    'host': "localhost", 
    'user': "root",    
    'password': "superbaba@123",
    'database': "eesha" # Ensure this database and the Star Schema tables exist
}

# --- HYBRIDJOIN PARAMETERS AND SHARED DATA STRUCTURES ---

# HybridJoin Parameters (These define the size of the memory buffer H)
H_slots = 10000        # Number of slots in the Hash Table 
VP = 500               # Disk Buffer size
trans_chunk = 100      # No of transactional rows to load at once

stream_Buff = queue.Queue() # Holds streaning transactional data
DW_load_Q = queue.Queue()   # Holds enriched Fact_Sales records

# Dictionaries for

Customer_MD = {}
Product_MD = {}
T_Dim = {} # Stores unique dates encountered for Dim_Time

# HYBRIDJOIN ---------------------------------------------------------------------

# H: Hash Table (a dictionary mapping Customer_ID -> list of Stream_Tuples)
Hash_T = {} 

# Queue having Customer_IDs, track the "oldest" stream data
Hybrid_Q = deque() 

# Number of available slots in the hash table
W = H_slots 

# Concurrency Control
LOCK = threading.Lock()        # Lock for protecting shared resources (H, Q, w)
STOP_EVENT = threading.Event() # Event to signal threads to safely stop execution


# HELPER FUNCTIONS ------------------------------------------------------------------

def Time_attr_generating(date_str):
    # Generates time dimension attributes from a date string
    try:
        # Use simple error handling for date parsing
        date = pd.to_datetime(date_str)
        is_weekend = date.weekday() >= 5 # 5=Saturday, 6=Sunday
        return {
            "Date_Key": date.strftime('%Y-%m-%d'),
            "Full_Date": date.strftime('%Y-%m-%d %H:%M:%S'),
            "Month": int(date.month),
            "Quarter": int(date.quarter),
            "Year": int(date.year),
            "Day_Name": date.strftime('%A'),
            "Is_Weekend": is_weekend
        }
    except Exception as e:
        print(f"Error processing date {date_str}: {e}")
        return None

def MD_load_func(customer_path, product_path):
    
    print("Loading Master Data (Simulating Indexed Disk Relation R)...")
    
    # 1. Load Customer Master Data 

    df_customer = pd.read_csv(customer_path)
    global Customer_MD
    for index, row in df_customer.iterrows():
        Customer_ID = row['Customer_ID']
        Customer_MD[Customer_ID] = {
            'Gender': row['Gender'],
            'Age': row['Age'],
            'Occupation': row['Occupation'],
            'City_Category': row['City_Category'],
            'Stay_In_Current_City_Years': row['Stay_In_Current_City_Years'],
            'Marital_Status': row['Marital_Status']
        }
    print(f"Loaded {len(Customer_MD)} unique customer entries.")

    # 2. Load Product Master Data 

    df_product = pd.read_csv(product_path)
    global Product_MD
    for index, row in df_product.iterrows():
        product_id = row['Product_ID']
        Product_MD[product_id] = {
            'Product_Category': row['Product_Category'],
            'Unit_Price': row['price$'],
            'Store_ID_FK': row['storeID'],
            'Store_Name': row['storeName'],
            'Supplier_ID_FK': row['supplierID'],
            'Supplier_Name': row['supplierName'],
            'Product_Name': f"Product {product_id} Category {row['Product_Category']}" 
        }
    print(f"Loaded {len(Product_MD)} unique product entries.")
    print("Master Data Loading Complete.")

# THREAD 1: STREAM LOADER --------------------------------------------

def stream_loader_thread1_func(transactional_path):
    
    print("\n[STREAM LOADER] Thread Started.")
    
    # Read the transactional data in chunks to simulate continuous arrival
    try:
        for chunk in pd.read_csv(transactional_path, chunksize=trans_chunk):
            if STOP_EVENT.is_set():
                break

            for _, row in chunk.iterrows():
                if STOP_EVENT.is_set():
                    break
                    
                transaction_tuple = row.to_dict()
                
                stream_Buff.put(transaction_tuple)
                
                # Pre-process Time Dimension data for further working

                date_key = Time_attr_generating(transaction_tuple['date'])['Date_Key']
                T_Dim[date_key] = Time_attr_generating(transaction_tuple['date'])
            
            time.sleep(0.01)

    except FileNotFoundError:
        print(f"[STREAM LOADER] ERROR: Transactional file not found at {transactional_path}")
    except Exception as e:
        print(f"[STREAM LOADER] An unexpected error occurred: {e}")
        
    print("[STREAM LOADER] Finished processing all transactional data.")
    
    # Give the processor a moment to empty the queue, then signal it to stop
    STOP_EVENT.set() # Set the stop flag once the stream is depleted.

# THREAD 2: HYBRIDJOIN PROCESSOR (ETL) -------------------------------------------------------------

def hybrid_join_processor_thread2_func():
   
    global W, Hash_T, Hybrid_Q

    print("\n[HYBRIDJOIN] Processor Thread Started.")
    print(f"[HYBRIDJOIN] Initialized with H capacity: {H_slots} slots. w = {W}")

    # Until stream is stopped or buffers are empty 
    while not STOP_EVENT.is_set() or not stream_Buff.empty() or Hybrid_Q:
        
        # Load Stream Data into Hash Table 
        # The goal is to fill available slots (w) in the hash table.

        with LOCK:
            loaded_count = 0
            while W > 0 and not stream_Buff.empty():
                
                raw_tuple = stream_Buff.get()
                Customer_ID = raw_tuple['Customer_ID']
                
                # Map the Customer_ID to a slot in H for look ups

                if Customer_ID not in Hash_T:
                    # Initialize list for collision chaining (multiple orders per customer)
                    Hash_T[Customer_ID] = []
                
                # Add the raw tuple to the Hash Table (H)

                Hash_T[Customer_ID].append(raw_tuple)

                # Add the key to the hybrid Queue 

                Hybrid_Q.append(Customer_ID)
                
                W -= 1
                loaded_count += 1
            
            if loaded_count > 0:
                print(f"[HYBRIDJOIN] Loaded {loaded_count} tuples from stream. Current H size: {len(Hybrid_Q)}. Remaining w: {W}")
            
        # If no stream data and no data in the hash table then just break he loop

        if not Hybrid_Q and stream_Buff.empty() and STOP_EVENT.is_set():
            break

        # Load Disk Partition (R) into Disk Buffer (vP) ---
        
        target_Customer_ID = None
        with LOCK:
            if Hybrid_Q:
                # ensures the oldest data is processed first 

                target_Customer_ID = Hybrid_Q[0]
            else:
                # Wait for the stream loader to provide data

                if STOP_EVENT.is_set() and stream_Buff.empty():
                    break
                time.sleep(0.05)
                continue
        
        # Simulate Disk Buffer (vP) containing the necessary Master Data (R)
        Disk_Buf_customer = Customer_MD
        Disk_Buf_product = Product_MD
        
        # Check Disk Chunk (R) vs. Hash Table (H) and Enrich ---
        
        join_count = 0
        vacated_slots = 0
        
        with LOCK:
            # Only proceed if the target customer is still in the hash table 

            if target_Customer_ID not in Hash_T:
                if Hybrid_Q and Hybrid_Q[0] == target_Customer_ID:
                    Hybrid_Q.popleft()
                continue

            stream_tuples_to_check = Hash_T[target_Customer_ID]
            customer_dims = Disk_Buf_customer.get(target_Customer_ID)
                
            if customer_dims:
                customer_new_list = [] # To hold any unmatched tuples 
                    
                for raw_tuple in stream_tuples_to_check:
                    product_id = raw_tuple['Product_ID']
                    product_dims = Disk_Buf_product.get(product_id)
                        
                    if product_dims:

                        # DATA ENRICHMENT / JOINING ---
                        enriched_record = {
                            "Order_ID": raw_tuple['orderID'],
                            "Product_ID_FK": product_id,
                            "Customer_ID_FK": target_Customer_ID,
                            "Quantity_Sold": raw_tuple['quantity'],
                        }

                        enriched_record.update(customer_dims)

                        # Enrich with Product/Store/Supplier Dimensions
                        enriched_record['Store_ID_FK'] = product_dims['Store_ID_FK']
                        enriched_record['Supplier_ID_FK'] = product_dims['Supplier_ID_FK']
                        enriched_record['Unit_Price'] = product_dims['Unit_Price']

                        # Calculate Fact Measures
                        enriched_record['Revenue'] = round(enriched_record['Quantity_Sold'] * enriched_record['Unit_Price'], 2)
                        
                        # Enrich with Time Dimension Key
                        time_data = Time_attr_generating(raw_tuple['date'])
                        enriched_record['Date_Key_FK'] = time_data['Date_Key']
                            
                        # Loading the final enriched record into the DW Load Queue
                        DW_load_Q.put(enriched_record)
                        join_count += 1
                        vacated_slots += 1
                    else:
                        customer_new_list.append(raw_tuple)
                            
                # Remove all matched tuples from the Hash Table 
                if not customer_new_list:
                    del Hash_T[target_Customer_ID]
                else:
                    Hash_T[target_Customer_ID] = customer_new_list
                    vacated_slots -= (len(stream_tuples_to_check) - len(customer_new_list))
                if Hybrid_Q and Hybrid_Q[0] == target_Customer_ID:
                     Hybrid_Q.popleft()

            else:
                print(f"[HYBRIDJOIN] WARNING: Master Data missing for Customer {target_Customer_ID}. Stream data persists.")
                
                if Hybrid_Q and Hybrid_Q[0] == target_Customer_ID:
                    Hybrid_Q.popleft()
                continue
                
            # Update W 
            W += vacated_slots
            if vacated_slots > 0:
                print(f"[HYBRIDJOIN] Joined {join_count} records. Vacated {vacated_slots} slots. New w: {W}. H size: {len(Hybrid_Q)}")
            
        # Prevent the busy-waiting loop from consuming all CPU
        if stream_Buff.empty() and not Hybrid_Q:
            time.sleep(0.01)

    print("\n[HYBRIDJOIN] Processor finished. Cleaning up.")

# DATAFRAME CREATION -------------------------------------------------------

def create_dw_dataframes_func():
    # Takes enriched data from DW_load_Q and organizes it into Dimension and Fact DataFrames for loading into MySQL.
   
    #  Load Fact Table (Fact_Sales)
    enriched_records = []
    while not DW_load_Q.empty():
        record = DW_load_Q.get()
        # Filter the record to include only the Fact measures and Foreign Keys (FKs)
        fact_record = {
            'Order_ID': record['Order_ID'],
            'Product_ID_FK': record['Product_ID_FK'],
            'Customer_ID_FK': record['Customer_ID_FK'],
            'Store_ID_FK': record['Store_ID_FK'],
            'Supplier_ID_FK': record['Supplier_ID_FK'],
            'Date_Key_FK': record['Date_Key_FK'],
            'Quantity_Sold': record['Quantity_Sold'],
            'Unit_Price': record['Unit_Price'],
            'Revenue': record['Revenue'],
        }
        enriched_records.append(fact_record)
        
    df_fact_sales = pd.DataFrame(enriched_records)

    # Prepare Dimension Tables
    
    # Dim_Time: Uses pre-processed time data
    df_time = pd.DataFrame(T_Dim.values()).drop_duplicates(subset=['Date_Key'])
    
    # Dim_Customer -- Uses Customer_MD
    df_customer = pd.DataFrame.from_dict(Customer_MD, orient='index').reset_index().rename(columns={'index': 'Customer_ID'})
    df_customer['Occupation'] = df_customer['Occupation'].astype(str) 
    df_customer['Marital_Status'] = df_customer['Marital_Status'].astype(str)

    # Dim_Product/Store/Supplier --  Use Product_MD
    product_records = list(Product_MD.items())
    
    dim_product_data = [{'Product_ID': pid, 'Product_Category': d['Product_Category'], 'Product_Name': d['Product_Name']} for pid, d in product_records]
    df_product = pd.DataFrame(dim_product_data).drop_duplicates(subset=['Product_ID'])
    df_product['Product_Category'] = df_product['Product_Category'].astype(str) 

    dim_store_data = [{'Store_ID': d['Store_ID_FK'], 'Store_Name': d['Store_Name']} for _, d in product_records]
    df_store = pd.DataFrame(dim_store_data).drop_duplicates(subset=['Store_ID'])
    
    dim_supplier_data = [{'Supplier_ID': d['Supplier_ID_FK'], 'Supplier_Name': d['Supplier_Name']} for _, d in product_records]
    df_supplier = pd.DataFrame(dim_supplier_data).drop_duplicates(subset=['Supplier_ID'])
    
    print(f"\nPrepared {len(df_time)} Dim_Time, {len(df_customer)} Dim_Customer, {len(df_product)} Dim_Product, {len(df_store)} Dim_Store, and {len(df_supplier)} Dim_Supplier rows.")
    print(f"Prepared {len(df_fact_sales)} enriched records for Fact_Sales.")
    
    # Return dictionary of all tables
    return {
        "Fact_Sales": df_fact_sales,
        "Dim_Customer": df_customer,
        "Dim_Product": df_product,
        "Dim_Time": df_time,
        "Dim_Store": df_store,
        "Dim_Supplier": df_supplier,
    }

# MYSQL LOAD FUNCTION -------------------------------------------

def create_db_connection():
    # Establishes connection to the MySQL database.
    try:
        conn = mysql.connector.connect(**db_configuration)
        return conn
    except mysql.connector.Error as err:
        print(f"\n[DW LOADER] CRITICAL ERROR: Failed to connect to MySQL: {err}")
        print("Please check your db_configuration (host, user, password, database) and ensure MySQL is running.")
        return None

def load_data_to_mysql(dw_tables):
    """Loads all prepared DataFrames into the respective MySQL tables."""
    print("\n[DW LOADER] Starting MySQL Load...")
    conn = create_db_connection()
    if not conn:
        return # if connection fails

    cursor = conn.cursor()
    
    # Column names should match your MySQL table schema order EXACTLY.
    load_order = [
        ("Dim_Time", ["Date_Key", "Full_Date", "Month", "Quarter", "Year", "Day_Name", "Is_Weekend"]),
        
        ("Dim_Customer", ["Customer_ID", "Gender", "Age", "Occupation", "City_Category", "Stay_In_Current_City_Years", "Marital_Status"]),
        
        ("Dim_Product", ["Product_ID", "Product_Category", "Product_Name"]),
        ("Dim_Store", ["Store_ID", "Store_Name"]),
        ("Dim_Supplier", ["Supplier_ID", "Supplier_Name"]),
        
        ("Fact_Sales", ["Order_ID", "Product_ID_FK", "Customer_ID_FK", "Store_ID_FK", "Supplier_ID_FK", "Date_Key_FK", "Quantity_Sold", "Unit_Price", "Revenue"])
    ]
    
    try:
        # CRITICAL STEP: TRUNCATE Fact Table FIRST to release Dimension FKs
        print("[DW LOADER] Truncating Fact_Sales table...")
        cursor.execute("TRUNCATE TABLE Fact_Sales")

        # Insert Data
        for table_name, columns in load_order:
            df = dw_tables.get(table_name)
            if df is None or df.empty:
                continue
                
            cols_str = ", ".join(columns)
            vals_str = ", ".join(["%s"] * len(columns))
            
            # Use REPLACE INTO for Dimensions to handle repeated runs/updates
            if table_name != "Fact_Sales":
                sql = f"REPLACE INTO {table_name} ({cols_str}) VALUES ({vals_str})"
                print(f"[DW LOADER] Using REPLACE INTO for {table_name}...")
            else:
                # Use INSERT INTO for Fact_Sales
                sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str})"
                print(f"[DW LOADER] Using INSERT INTO for {table_name}...")

            # Convert DataFrame rows to tuples for insertion
            data_to_insert = [tuple(row) for row in df[columns].values]
            
            print(f"[DW LOADER] Attempting to load {len(data_to_insert)} rows into {table_name}...")

            cursor.executemany(sql, data_to_insert)

            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            rows_in_db = cursor.fetchone()[0]

            if rows_in_db == 0 and len(data_to_insert) > 0:

                print(f"[DW LOADER] FAILED VERIFICATION: {table_name} has 0 rows after loading! Check MySQL schema/data types.")
            
            
            elif rows_in_db > 0:
                print(f"[DW LOADER] Successfully inserted {rows_in_db} rows into {table_name}.")

        # Commit the transaction
        conn.commit()
        print("[DW LOADER] MySQL Load Complete: Transaction committed.")
        
    except mysql.connector.Error as err:
        print(f"[DW LOADER] CRITICAL ERROR during insertion: {err}")
        print(f"Error occurred during insertion into table: {table_name}")
        conn.rollback() # Rollback if any insertion fails
        print("[DW LOADER] Transaction rolled back.")

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
    
# MAIN EXECUTION ----------------------------------------------------------------------------

def run_hybridjoin_etl():
    
    # Define file paths
    Customer_MD_file = 'customer_master_data.csv'
    Product_MD_file = 'product_master_data.csv'
    transaction_file = 'transactional_data.csv'

    # Check whether files exist 
    if not all(os.path.exists(f) for f in [Customer_MD_file, Product_MD_file, transaction_file]):
        print("ERROR: One or more required CSV files are missing. Please ensure the mock data files are created.")
        return None
        
    # Load Master Data 
    MD_load_func(Customer_MD_file, Product_MD_file)

    # Start Concurrency (ETL Processing)
    loader = threading.Thread(target=stream_loader_thread1_func, args=(transaction_file,))
    processor = threading.Thread(target=hybrid_join_processor_thread2_func)
    
    loader.start()
    processor.start()
    
    print("\n--- Waiting for ETL process to complete (Stream Depletion) ---")
    
    # Wait for the threads to finish
    loader.join()
    processor.join()
    
    print("\n--- ETL Process Complete. Starting DW Load Simulation ---")
    
    # Load data from DW_load_Q and prepare for analysis
    dw_tables = create_dw_dataframes_func()

    # Load DataFrames into MySQL
    load_data_to_mysql(dw_tables)
    
    print("\n*** HYBRIDJOIN ETL successfully enriched and loaded data. ***")
    
    return dw_tables

if __name__ == "__main__":
    print("--- STARTING HYBRID JOIN ETL PROCESS ---")
    print("WARNING: Update db_configuration with your MySQL credentials")
    final_data_for_analysis = run_hybridjoin_etl()
    

