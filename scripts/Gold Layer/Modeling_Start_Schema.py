"""
# ===============================================================================
# PySpark Script: Create Gold Table
# ===============================================================================
# Script Purpose:
#     This script creates a table for the Gold layer in the data warehouse. 
#     The Gold layer represents the final dimension and fact tables (Star Schema)

#     Each table performs transformations and combines data from the Silver layer 
#     to produce a clean, enriched, and business-ready dataset.

#     The model includes 3 dimensions and 1 fact table:
#      1- Customer_Dim
#      2- Product_Dim
#      3- Date_Dim
#      4- Fact_Sales

# Usage:
#     - These Tables can be queried directly for analytics and reporting.
# ===============================================================================

"""
import pyspark
from pyspark.sql import SparkSession 
spark = SparkSession.builder \
    .appName("Business_logic") \
    .getOrCreate()

# -- ======================= Create JDBC [Java DataBase Connection] ======================================================

# step 1: Analyze: Explore  and read to understand data

jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=DWh;encrypt=true;trustServerCertificate=true"
connection_props = {
    "user": "sa",
    "password": "************!",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# 3. read all table
tables = ["silver.crm_Cust_info", "silver.crm_prd_info","silver.crm_sales_details",\
          "silver.erp_cust_az12","silver.erp_loc_a101","silver.erp_px_cat_g1v2"]

# 4. loop in DB tables, read all tables, and save in dictionary for readability
silver_dfs = {}                                                                     # {table, data frame}
for t in tables: 
   silver_dfs[t] = spark.read.jdbc(url=jdbc_url, table=t, properties=connection_props)
   print(f" Loaded {t} : {silver_dfs[t].count()} rows")

# -- =============================================================================
# 1- Create Dimension: gold.dim_customers
# -- we add [start_date, end_date , is_current ] >> for tracking the history changes.
# -- =============================================================================

crm_cust = silver_dfs["silver.crm_Cust_info"]
erp_cust = silver_dfs["silver.erp_cust_az12"]
erp_loc = silver_dfs["silver.erp_loc_a101"]
from pyspark.sql.functions import monotonically_increasing_id, current_date, lit, col,count,when ,coalesce ,window,max

#----------  A- create surrogate key ----------------

# create business key / full join or left join with master table (crm_cust)
# [maybe we have a customer with no location or bdate]

customer = crm_cust.join(erp_cust, crm_cust["cst_key"] == erp_cust["CID"], "left")\
    .join(erp_loc, crm_cust["cst_key"] == erp_loc["CID"], "left")\
    .withColumn(
    "cust_Sk", monotonically_increasing_id()+1)\     
    .withColumn("start_date", current_date())\
    .withColumn("end_date", lit(None).cast("date"))\
    .withColumn("is_current", lit(1))
   
# we have two gender columns, the priority is:crm_customer table, not erp  
customer.select("cst_gndr","GEN").distinct()  

# replace the crm_gender with value  "n/a" with erp_cust Gender but if null lit n/a   

customer_dim_gender = customer.withColumn("Gender", when(col("cst_gndr") != "n/a", col("cst_gndr"))\
                                   .otherwise(coalesce(col("GEN"),lit("n/a"))))

customer_dim_gender.select("Gender","cst_gndr","GEN")    # done
customer_dim_gender

#-------------------------------- create Full Dimension with friendly name ----------------
 
  
customer_dim= customer_dim_gender.select(
            "cust_Sk"
            ,col("cst_id").alias("Customer_Id")
            ,col("cst_key").alias("customer_number")     
            ,col("cst_firstname").alias("First_name")
            ,col("cst_lastname").alias("Last_name")
            ,col("cntry").alias("Country")
            ,col("cst_marital_status").alias("marital_status")
            ,col("Gender")\
            ,col("bdate").alias("Birthdate")
            ,col("cst_create_date").alias("customer_create_date")
            ,col("start_date")
            ,col("end_date")
            ,col("is_current"))

customer_dim.show()

# check join not produce duplicate 
# customer_dim.groupby("cst_key").agg(count("*").alias("dp")).filter(col("dp")>1)   # no result

# -- ===========================================================================================
                              # 2- Create Dimension: gold.dim_Product
# -- we already have history in the silver layer >> so, compare the values if change >> Update 
# -- ===========================================================================================

# step 1: Analyze: Explore  and read to understand data

crm_prd = silver_dfs["silver.crm_prd_info"]
erp_prd = silver_dfs["silver.erp_px_cat_g1v2"]

# 1- join and find if we have a product with no cat and a cat with no product

crm_prd.join(erp_prd, crm_prd["cat_id"]== erp_prd["ID"], "left_anti").show()  # 7 product with NULL category [(CO_PE)]


erp_prd.join(crm_prd, crm_prd["cat_id"]== erp_prd["ID"], "left_anti").show()  # 1 Category with NULL Product [(CO_PD)]

# Decision: ignore the category until products are assigned to it

product_dim_join = crm_prd.join(erp_prd, crm_prd["cat_id"]== erp_prd["ID"], "left")

# after join we have null in start date & end date > we add condition\ if the start date & end date is null > current 0

product_dim = product_dim_join.withColumn(
        "Product_SK", monotonically_increasing_id()+1)\
        .withColumn(
        "Is_Current",  when(col("prd_start_dt").isNull(),(0))
                        .when((col("prd_end_dt").isNotNull()),0)\
                        .otherwise(1)).select(
        "Product_SK",
        col("prd_id").alias("Product_id"),
        col("prd_key").alias("Product_Key"),        
        col("prd_nm").alias("Product_name"),        
        col("cat_id").alias("Category_id"),
        col("CAT").alias("Category"),
        col("SUBCAT").alias("Sub_Category"),
        col("MAINTENANCE").alias("Maintenance"),
        col("prd_cost").alias("Cost"),
        col("prd_line").alias("Product_line"),
        col("prd_start_dt").alias("start_dt"),
        col("prd_end_dt").alias("end_dt"),
        col("Is_Current")
        )

print(crm_prd.count())        # 397
print(product_dim.count())    # 397, we have 1 cat with non product sk= 280
print(product_dim.filter(col("end_dt").isNotNull()).count())    # 102 historical

# -- ========================================================================================
                                # 3- Create Dimension: gold.dim_Date
                                # -- we will create this LOOKUP from scratch
# -- ==========================================================================================


#  CREATE DIMENSION ----- 3- Date ---
from pyspark.sql.functions import max,min, window
from pyspark.sql.window import Window
from pyspark.sql.functions import col, year, month, dayofmonth, weekofyear, quarter, date_format
from pyspark.sql.types import DateType
import datetime 

# step 1: Analyze: Explore  and read to understand data

# Determine the range [2002 - 2017] / 15 years
# x = silver_dfs["silver.crm_sales_details"]
# product_dim.agg(max("start_dt"), min("start_dt")).show()  # 2003 - 2013
# product_dim.agg(max("end_dt"), min("end_dt")).show()      # 2012 - 2013
# x.agg(max("sls_order_dt"),min("sls_order_dt")).show()     # 2010 - 2014
# x.agg(max("sls_ship_dt"),min("sls_ship_dt")).show()     # 2011 - 2014
# x.agg(max("sls_due_dt"),min("sls_due_dt")).show()     # 2011 - 2014

# ------------------------------ Date Variable ------------------------
# 1. Define start and end date

start_date = datetime.date(2003,1,1)
end_date = datetime.date(2017,12,31)

# 2. Generate list of dates
date_list = [(start_date + datetime.timedelta(days=x)) for x in range((end_date - start_date).days + 1)]

""" 
Explanation -Example- 
end_date - start_date).days + 1) >>>> ex 365-1 + 1 = 365   [1-1-2010 , 5-1-2010] = 5 day
x = [0,1,2,3,4]
start_date + datetime.timedelta(days=x)) >> 1-1-2010 + 0 = 1-1-2010 
                                         >> 1-1-2010 + 1 = 2-1-2010
                                         >> 1-1-2010 + 2 = 3-1-2010
                                         >> 1-1-2010 + 3 = 4-1-2010
                                         >> 1-1-2010 + 4 = 5-1-2010
timedelta > different 
"""

# 3. Convert to Spark DataFrame
date_df = spark.createDataFrame(date_list, DateType()).toDF("full_date")

# 4. Add columns 


date_dim = date_df.withColumn("date_sk",\
                     date_format(col("full_date"), "yyyyMMdd").cast("int"))\
                  .select(
                      col("date_sk"),          # surrogate key
                      col("full_date"),       
                      year("full_date").alias("year"),
                      month("full_date").alias("month"),
                      dayofmonth("full_date").alias("day"),
                      quarter("full_date").alias("quarter"),  
                      weekofyear("full_date").alias("week_of_year"),
                      date_format("full_date", "E").alias("day_name"),
                      date_format("full_date", "MMMM").alias("month_name"),
                      when(date_format(col("full_date"), "E").isin("Sat", "Sun"), lit(1))\
                      .otherwise(lit(0)).alias("is_weekend"))

date_dim.show()
print(date_dim.count())
date_dim.agg(max("date_sk")).show()

# -- ========================================================================================
                                # 4- Create Fact table: gold.Fact_sales
                                # -- we will join this table with dimensions
# -- ==========================================================================================
from pyspark.sql.functions import max,min, window, sum 
from pyspark.sql.window import Window 
from pyspark.sql.functions import row_number
from pyspark.sql.functions import count, col


# step 1: Analyze: Explore  and read to understand data

fact = silver_dfs["silver.crm_sales_details"]
# fact.show(1)
# product_dim.show(1)
# customer_dim.show(1)
# print(fact.count())                     #60398

"""
# relation between Product_dim & sales  is M to M [many row(one product) - sealed many time] 
# sales["sls_prd_key] == Product_dim["Product_Key"] 
# relation between Customer_dim & sales  is one to many 
# sales["sls_prd_key] == Customer_dim["Product_Key"] 

------------ product --------------------------------------
# the relation is one - many [one product has many sales]/ 
 ******** and we have a historical record for each product

#  We use SKs instead of IDs to easily connect facts with the dimension
-----------------------------------------------------------------
"""

 
 # the best: left join  / ex, product sealed but without customer info  
 # after alias must call the column with col() function 
 # Adding condition where is_current == 1 for decreasing duplicates and working in the current only
fact_tb = (
    fact.join(product_dim, (fact["sls_prd_key"] == product_dim["Product_Key"]) & (product_dim["Is_Current"] == 1), "left")
        .join(customer_dim, fact["sls_cust_id"] == customer_dim["Customer_Id"], "left")
        .join(date_dim.alias("order_date"), fact["sls_order_dt"] == col("order_date.full_date"), "left")
        .join(date_dim.alias("ship_date"), fact["sls_ship_dt"] == col("ship_date.full_date"), "left")
        .join(date_dim.alias("due_date"), fact["sls_due_dt"] == col("due_date.full_date"), "left")
)



fact_sales = fact_tb.withColumn("Sales_Sk", monotonically_increasing_id() +1)\
    .select(
    col("Sales_Sk"),
    col("sls_ord_num").alias("Order_number"),
    col("cust_Sk").alias("Customer_FK"),
    col("Product_SK").alias("Product_FK"),
    col("order_date.date_sk").alias("Order_date_FK"),
    col("Ship_date.date_sk").alias("Ship_date_FK"),
    col("due_date.date_sk").alias("Due_date_FK"),
    col("sls_price").alias("Unit_Price"),
    col("sls_quantity").alias("Quanity"),
    col("sls_sales").alias("Total_sales")
)

