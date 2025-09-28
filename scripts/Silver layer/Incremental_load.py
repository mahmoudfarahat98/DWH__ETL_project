## -------------------------------- Load to Silver Layer ["incremental load - once - "] ------------------------------##
""" 
  1. staging step 
    A. Make a connection jdbc
    B. Read the table from the database

  2. joining step   
    A.join the transformed data {silver dict } with readed table {silver_tables}
      - left join, left_anti
      - create da ict to save incremental dfs 

  3. loading  
    A. Append the row found in the source and not found 

** important note: We have 3 dict :
  **  The result of the transformation is: Silver_dict
  **  The result of reading the silver table is: Silver_tables
  ** left join of silver_dict with silver_tables >>> new row data needs to * Append *: saved 
      in silver_incremental_dict 
  
"""
## --------------------------------   A. Make a connection jdbc-----------------------------------------------------------------##
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=DWh;encrypt=true;trustServerCertificate=true"
connection_props = {
    "user": "sa",
    "password": "YourStrongPassword1!",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}

# . read all table
tables = ["silver.crm_Cust_info", "silver.crm_prd_info","silver.crm_sales_details",\
          "silver.erp_cust_az12", "silver.erp_loc_a101", "silver.erp_px_cat_g1v2"]

## --------------------------------  1. incr crm_customer_info -----------------------------------------------------------------##

# Step 1: Build incremental dict
silver_incremental_dict = {}                             # save all incremental dfs

# 2. loop in DB tables, read all tables, and save in a dictionary for readability
silver_tables = {}                                        # {table , data frame}
for t in tables: 
    #jdbc is alternative way for read takes 3 argument (url = str, table = str, properties = dict[username, password, driver])
    silver_tables[t] = spark.read.jdbc(url=jdbc_url, table=t, properties=connection_props)
    
 # join two table   
source_crm_cust= silver_dict["silver.crm_Cust_info"]      # Transformed table 
target_crm_cust = silver_tables["silver.crm_Cust_info"]   # readed table

 # saving 
silver_incremental_dict["silver.crm_Cust_info"]= source_crm_cust.join(target_crm_cust, "cst_id" ,"left_anti") # incr df

## --------------------------------  2. incr crm_prd_info -----------------------------------------------------------------##
    
 # join two table   
source_crm_prd= silver_dict["silver.crm_prd_info"]     # Transformed table 
target_crm_prd = silver_tables["silver.crm_prd_info"]  # readed table

silver_incremental_dict["silver.crm_prd_info"]= source_crm_prd.join(target_crm_prd, "prd_id" ,"left_anti") # incr df


## --------------------------------  3. incr crm_sales_details -----------------------------------------------------------------##
    
 # join two table   
source_crm_sales= silver_dict["silver.crm_sales_details"]           # Transformed table 
target_crm_sales = silver_tables["silver.crm_sales_details"]        # readed table

silver_incremental_dict["silver.crm_sales_details"]= source_crm_sales.join(target_crm_sales, "sls_ord_num" ,"left_anti") # incr df

## --------------------------------  4. incr erp_cust_az12 -----------------------------------------------------------------##
    
 # join two table   

source_erp_cust= silver_dict["silver.erp_cust_az12"]    # Transformed table 
target_erp_cust = silver_tables["silver.erp_cust_az12"]  # readed table

silver_incremental_dict["silver.erp_cust_az12"]= source_erp_cust.join(source_erp_cust, "CID" ,"left_anti")       # incr df


## --------------------------------  5. incr erp_loc_a101 -----------------------------------------------------------------##
    
 # join two table   

source_erp_loc= silver_dict["silver.erp_loc_a101"]          # Transformed table 
target_erp_loc = silver_tables["silver.erp_loc_a101"]       # readed table

silver_incremental_dict["silver.erp_loc_a101"]= source_erp_loc.join(target_erp_loc, "CID" ,"left_anti") # incr df

## --------------------------------  6. incr erp_px_cat_g1v2 -----------------------------------------------------------------##
    
 # join two table   

source_erp_px= silver_dict["silver.erp_px_cat_g1v2"]        # Transformed table 
target_erp_px = silver_tables["silver.erp_px_cat_g1v2"]     # readed table

silver_incremental_dict["silver.erp_px_cat_g1v2"]= source_erp_px.join(target_erp_px, "ID" ,"left_anti")  # incr df
 
## --------------------------------   @ loading  ----------------------------------------------
#                               A. append the row found in source and not found  -----------------------------------------------------------------##

incr_batch_load_str= time.time()
for tablename , value in silver_incremental_dict.items():

# 6. append only this table
    try:
        loading_start_time = time.time()
        inserted_rows = silver_incremental_dict[tablename]

        source_count = inserted_rows.count()                   # count the inserted
        start_incr_load = time.time()
        inserted_rows.write.jdbc(url = jdbc_url,\
                                table = tablename, \
                                mode="append", \              ##### * Append not over write
                                properties = connection_props)
        
        loading_end_time = time.time()
        table_inc_loading_time = loading_end_time - loading_start_time

        print(f"successfully incremental load {tablename} count : {source_count} rows in {table_inc_loading_time} second ")
    except:
        print(f"error in loadeing {tablename}")

incr_batch_load_end= time.time()
incr_batch_load= incr_batch_load_end - incr_batch_load_str
print(f"succesfully incremantal pipeline load in {incr_batch_load} second")


print(silver_incremental_dict.keys())
