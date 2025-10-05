#--------------------------- SCD type 2 -------------------------#
""" 
We have 3 dict, the module after transformation and before loading to the target  >> {star_schema}
& Read the destination [customer, product, fact]                                  >> {target_dim}
& the updated rows will be loading to gold again                                  >> {incremental_dict}

"""
incremental_dict={}
target_dim = {}
dim_list = ["fact_table","customer_dim","product_dim"]

for i in dim_list :
    target_dim[i] = spark.read.jdbc(url=jdbc_url, table= f"Gold.{i}" , properties= connection_props)
    count= target_dim[i].count()
    print(f"Succses loading {i}, {count} rows")

""" 
2- join the source (Star_schema{}) Dim with distination dimension target_dim
"""
#--------------------------- 1 - Customer -DIM -------------------------#
target_cust=target_dim["customer_dim"]
source_cust= Star_schema["customer_dim"]   # after model and before load

customer_join = source_cust.alias("sc").join(target_cust.alias("tc"),col("sc.customer_number") == col("tc.customer_number") ,"left")

print(target_cust.count())
print(source_cust.count())
# new Customer
new_customer = customer_join.filter(col("tc.customer_number").isNull()).select("sc.*")

# print(new_customer.count())  # done

# No Change in any Customer info [[null != null ] so need need eqNullSafe()

Unchanged_customer = customer_join.filter (
                    (col("sc.Customer_Id").eqNullSafe(col("tc.Customer_Id")))
                    &(col("sc.customer_number").eqNullSafe(col("tc.customer_number")))
                    &(col("sc.First_name").eqNullSafe(col("tc.First_name")))
                    &(col("sc.Last_name").eqNullSafe(col("tc.Last_name")))
                    &(col("sc.Country").eqNullSafe(col("tc.Country")))
                    &(col("sc.marital_status").eqNullSafe(col("tc.marital_status")))
                    &(col("sc.Gender").eqNullSafe(col("tc.Gender")))
                    &(col("sc.Birthdate").eqNullSafe(col("tc.Birthdate")))                    
                    ).select("tc.*")


# Change in any Customer info 
changed_customer = customer_join.filter (
                     (col("tc.is_current") == 1 )
                    &((col("sc.Customer_Id") != col("tc.Customer_Id"))\
                    |(col("sc.customer_number") != col("tc.customer_number"))\
                    |(col("sc.First_name") != col("tc.First_name"))\
                    |(col("sc.Last_name") != col("tc.Last_name"))\
                    |(col("sc.Country") != col("tc.Country"))\
                    |(col("sc.marital_status") != col("tc.marital_status"))\
                    |(col("sc.Gender") != col("tc.Gender"))\
                    |(col("sc.Birthdate") != col("tc.Birthdate"))))
                                        
# Update the target end date when (source is_current(0) != target is_current (1))

update_target = changed_customer.select("tc.*")\
                                .withColumn("end_date",(current_date())) \
                                .withColumn("is_current",lit(0).cast("int"))
                                       
new_cust_info = changed_customer.select("sc.*")\
                                .withColumn("start_date",(current_date() )) \
                                .withColumn("is_current",lit(1).cast("int"))

                                       

# ----------- . Final Union -----------
incremental_dict["customer_dim"]= Unchanged_customer.unionByName(new_customer)\
                                .unionByName(update_target)\
                                .unionByName(new_cust_info)

# #--------------------------- 2 - Product-DIM -------------------------#
# """
# Steps 
#             1- join the two table [source + target]
#             2- new product with NULL Target Product key [new product]
#             3- clean product : { have the same row in both source & target }

# --  challengs 
# - the history id not affected because in system\
#     any change inserted in new row autamatically put end_date and start_date
#     so find the updated product by :
#     **- target is_current = 1 & 
#         1- the source is_current (0) != target is_current (1)>> update target end_date with s. end_date
#         2- the source start_dt != target start_dt >>  new source row > insert into target

# """

target_prd= target_dim["product_dim"]     # readed from target
source_prd= Star_schema["product_dim"]     # after model and before load

# target_prd.show(2)
# join two table
product_join = source_prd.alias("s").join(target_prd.alias("t"),"Product_id", "left")


# new product (prd_key not found in target) [Expected no result]
new_product = product_join.filter(col("t.Product_id").isNull()).select("s.*")

# cleaned-row product

cleaned_row_prd= product_join.filter((col("s.Product_id").eqNullSafe(col("t.Product_id")))\
                                & (col("s.Product_Key").eqNullSafe(col("t.Product_Key")))\
                                & (col("s.Product_name").eqNullSafe(col("t.Product_name")))\
                                & (col("s.Category_id").eqNullSafe(col("t.Category_id")))\
                                & (col("s.Category").eqNullSafe(col("t.Category")))\
                                & (col("s.Sub_Category").eqNullSafe(col("t.Sub_Category")))\
                                & (col("s.Maintenance").eqNullSafe(col("t.Maintenance")))\
                                & (col("s.Cost").eqNullSafe(col("t.Cost")))\
                                & (col("s.Product_line").eqNullSafe(col("t.Product_line")))\
                                & (col("s.start_dt").eqNullSafe(col("t.start_dt")))\
                                & (col("s.end_dt").eqNullSafe(col("t.end_dt")))).select("t.*")

# changed rows ["for current product ,[is_Current] not Equal, or change in any field >"]
# two Scenario , the date in source is closed > new row with new start date

changed_prd = product_join.filter((col("s.Product_Key") == col("t.Product_Key"))\
                                 & (col("t.Is_Current") == 1)
                                 & ((col("s.Is_Current") != col("t.Is_Current"))\
                                |(col("s.start_dt") != col("t.start_dt"))\
                                |(col("s.Product_name")!= col("t.Product_name"))\
                                |(col("s.Category_id")!= col("t.Category_id"))\
                                |(col("s.Category")!= col("t.Category"))\
                                |(col("s.Sub_Category")!= col("t.Sub_Category"))\
                                |(col("s.Maintenance")!= col("t.Maintenance"))\
                                |(col("s.Cost")!= col("t.Cost"))))


# 1- the source is_current(0) != target is_current (1)>> 
# update target end_date with s. end_date

to_update_prd = changed_prd.filter((col("s.Is_Current") != col("t.Is_Current"))&
                            (col("s.start_dt") == col("t.start_dt")))\
                            .select("t.*", col("s.end_dt").alias("sd"))\
                            .withColumn("Is_Current", lit(0).cast("int"))\
                            .withColumn("end_dt",col("sd")).drop("sd")  # colsed 


# if historical but not in dWH [strt != start , End!= end , Current!=current]
historical_rows_prd = changed_prd.filter(
                (col("t.Is_Current") == 1) & 
                (col("s.Is_Current") != col("t.Is_Current"))&
                (col("s.start_dt") != col("t.start_dt")) &
                (col("s.end_dt") != col("t.end_dt"))).select("s.*")

      
        
# 2- the source start_dt != target start_dt >> 
# new row for updated product 

new_version_prd = changed_prd.filter(((col("s.Is_Current") == col("t.Is_Current"))&
                             (col("s.start_dt") != col("t.start_dt"))))\
                             .select("s.*")\
                             .withColumn("start_dt", col("start_dt"))\
                             .withColumn("end_dt",lit(None).cast("date"))\
                             .withColumn("Is_Current", lit(1))

# ----------- 5. Final Union -----------
incremental_dict["product_dim"] = cleaned_row_prd.unionByName(new_product)\
                             .unionByName(to_update_prd)\
                             .unionByName(new_version_prd)\
                             .unionByName(historical_rows_prd)


# #--------------------------- 3- Fact -Table -------------------------#
source_fact= Star_schema["fact_table"]
target_fact= target_dim["fact_table"]

# identify new sales transactions from source
fact_join= source_fact.alias("sf").join(target_fact.alias("tf"), "Order_number","left_anti")
incremental_dict["fact_table"] = fact_join


# ----------- final step - loading -  -----------

import time

start_batch_time = time.time()

for Dimension , dimansion_DFs in incremental_dict.items():
    try:
        Count_Source = dimansion_DFs.count()
        start_load = time.time()

        if Dimension != "fact_table":
          
            dimansion_DFs.write.jdbc(jdbc_url, table=f"Gold.{Dimension}", mode= "overwrite", properties= connection_props)
        else:
            fact_join.write.jdbc(jdbc_url, table= f"Gold.{Dimension}", mode= "Append", properties= connection_props)

        end_load = time.time()
        loading_time = end_load - start_load 

        print(f"Succefully incremental loading {Dimension} rows : {Count_Source} in {loading_time} second")
    except Exception as e :
        print(f"Error in loading {Dimension}, {e}, {NameError}")
              

end_batch_time = time.time()
batch_load_time = end_batch_time - start_batch_time
print(f"loading Pipeline to Start schema take {batch_load_time} second")


