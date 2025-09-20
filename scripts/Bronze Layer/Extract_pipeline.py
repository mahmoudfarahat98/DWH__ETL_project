# /*
# ===============================================================================
# Extract Pipeline: Load Bronze Layer (Source -> Bronze)
# ===============================================================================
# Script Purpose:
#     This extracts and loads data into the 'bronze' schema from external CSV files. 
#     It performs the following actions:
#     - Truncates the bronze tables before loading data.
#     - Tracking the time-consuming tasks with a file for better performance
#     - overwrite to ensure the table is empty
#     - Track the count in source and target to ensure the quality of data and no missing rows.

# ===============================================================================

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType  # لو محتاج cast

# 1️⃣ إنشاء Spark session
spark = SparkSession.builder \
    .appName("Load_Bronze_ERP") \
    .getOrCreate()

# The first source system - CSVs file - ERP
folder_path = "/Users/mahmoudfarahat/Desktop/source_erp/"

#  mapping: اسم CSV -> اسم الجدول في SQL Server
file_table_map = {
    "CUST_AZ12.csv": "erp_cust_az12", # source file name and target table name
    "LOC_A101.csv": "erp_loc_a101",
    "PX_CAT_G1V2.csv": "erp_px_cat_g1v2"
}

# loop on the file mapping
for file, table_name in file_table_map.items():
    file_path = os.path.join(folder_path, file)
    table_name = f"bronze.{table_name}"

    start_time = time.time()   # track time taken for each file

    # Read CSV
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    source_count = df.count()  # count number of rows in source file
    
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:sqlserver://localhost:1433;databaseName=DWH;encrypt=true;trustServerCertificate=true") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("dbtable",table_name)\
            .option("user", "sa") \
            .option("password", "YourStrongPassword1!") \
            .mode("overwrite") \
            .save()
        
         # tracking the number of rows inserted by re-reading the target table
        df_target = spark.read.format("jdbc").options(
            url="jdbc:sqlserver://localhost:1433;databaseName=DWH;encrypt=true;trustServerCertificate=true",
            driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
            dbtable = table_name,
            user="sa",
            password="YourStrongPassword1!"
        ).load()


        end_time = time.time()
        elapsed_time = end_time - start_time
        target_count = df_target.count()
        print(f"Successfully inserted {file}: {source_count} row , Target {target_count} rows, Time taken is {elapsed_time:.2f} seconds")

    except Exception as e: # catch any error during insertion
        print(f"Error inserting data into {table_name}: {e}")
   ---
                                                         
 folder_path = "/Users/mahmoudfarahat/Desktop/source_crm/"

# 3️⃣ mapping: {souce name : target name }
file_table_map = {
    "cust_info.csv": "crm_Cust_info",
    "prd_info.csv": "crm_prd_info",
    "sales_details.csv": "crm_sales_details"
}

# 4️⃣ loop على كل CSV
for file, table_name in file_table_map.items():
    file_path = os.path.join(folder_path, file)
    table_name = f"bronze.{table_name}"              #schema name
    
    start_time = time.time()
    # Read the file

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    source_count = df.count()                      # count source row

    try:  # to ensure no errors 
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:sqlserver://localhost:1433;databaseName=DWH;encrypt=true;trustServerCertificate=true") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("dbtable", table_name) \
            .option("user", "sa") \
            .option("password", "YourStrongPassword1!") \
            .mode("overwrite") \
            .save()
        # print(f"Successfully inserted {df.count()} rows into {table_name} the number") # source_count to track number of rows in source fil
    # 5️⃣ tracking the number of rows inserted
        df_target = spark.read.format("jdbc").options(
            url="jdbc:sqlserver://localhost:1433;databaseName=DWh;encrypt=true;trustServerCertificate=true",
            driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
            dbtable=table_name,
            user="sa",
            password="YourStrongPassword1!"
        ).load()
        target_count = df_target.count()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Successfully inserted {file}: {source_count} row , Target {target_count} rows, Time taken is {elapsed_time:.2f} seconds")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
