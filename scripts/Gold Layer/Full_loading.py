#--------------------------- Loading [full load] -------------------------#
import time

Star_schema = {"fact_table": fact_sales,
               "customer_dim" : customer_dim,
               "product_dim": product_dim,
               "date_dim": date_dim}

start_batch_time = time.time()
for Dimension , dimansion_DFs in Star_schema.items():
    try:
        Count_Source = dimansion_DFs.count()
        start_load = time.time()
        dimansion_DFs.write.jdbc(jdbc_url, table=f"Gold.{Dimension}", mode= "overwrite", properties= connection_props)
        
        end_load = time.time()
        loading_time = end_load - start_load 

        print(f"Succefully loading Gold.{Dimension} rows : {Count_Source} in {loading_time} second")
    except Exception as e :
        print(f"Error in loading {Dimension}, {e}, {NameError}")
              
end_batch_time = time.time()
batch_load_time = end_batch_time - start_batch_time

print(f"loading to Start schema take {batch_load_time} second")
