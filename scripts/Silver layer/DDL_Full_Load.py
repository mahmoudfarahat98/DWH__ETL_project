## -------------------------------- Load to Silver Layer ["Full load - once - "] ------------------------------##
""" 
    A. The cleaned Data Frame saved in Dict {Silver Dict}
    B. First, we start the batch time to calculate the overall pipeline time consumed
    C. for loop in dict and add schema name silver. 
    d. We start Table time to calculate the load pipeline time consumed for each table
    E. create a connection with db and load 

    ** note:  silver_Dict is created in the transformation layer for assigning dataframes to the dictionary
      for usability and readability 
"""
## -------------------------------------------------------------------------------------------------##
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=DWh;encrypt=true;trustServerCertificate=true"
connection_props = {
    "user": "sa",
    "password": "YourStrongPassword1!",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

import time

start_load_silver_batch_time = time.time()                  # duration of loading silver

for table_name, df in silver_dict.items():                   
   
    try:                                                     # for error handling
        bronze_count = silver_dict[table_name].count()       # count number of rows in Bronze file
        table_start_time = time.time()                      # duration of loading each
        
        df.write.jdbc(url=jdbc_url, table= table_name, mode="overwrite",  properties=connection_props)
        table_end_time= time.time()
        
        table_time = table_end_time - table_start_time
        print(f"successfuly load {table_name} count : {bronze_count} in {table_time} second ")
    except:
        print(f"error in loading {table_name}")

    
end_load_silver_batch_time = time.time()
load_batch_time = end_load_silver_batch_time - start_load_silver_batch_time
print(f"Pipeline time is {load_batch_time} second")


