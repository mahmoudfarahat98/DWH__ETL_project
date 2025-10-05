"""
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    1. Using the stage area for transformation without affecting the  bronze layer 
    2. Reading the table from the silver layer by "jdbc" connection
    3. Transform all tables and check the quality after transformation to ensure quality and correctness.
    4. Because this load is the  first time load into the silver layer >> after transformation >>> full load, 
    and the next time:  incremental load script.
    5. The steps :
              A. Create a connection with the database
              B. loop in the table name and save the table in the dictionary  {dfs}
              C. Adding the time of ETL. 
              D. Clean the bronze table.
                / and Quality check after transformation
              E. Full Load into silver layer
===============================================================================
"""
import pyspark
from pyspark.sql import SparkSession 
spark = SparkSession.builder \
    .appName("Transform_Silver") \
    .getOrCreate()

# A. create connection
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=DWh;encrypt=true;trustServerCertificate=true"
connection_props = {
    "user": "sa",
    "password": "***************!",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# B. read all table
tables = ["bronze.crm_Cust_info", "bronze.crm_prd_info", "bronze.crm_sales_details",\
          "bronze.erp_cust_az12", "bronze.erp_loc_a101", "bronze.erp_px_cat_g1v2"]

# B. loop in DB tables, read all tables, and save in a dictionary for readability

dfs = {}                                         # {table, data frame}
for t in tables: 
    #jdbc is alternative way for read takes 3 argument (url = str, table = str, properties = dict[username, password, driver])
    dfs[t] = spark.read.jdbc(url=jdbc_url, table=t, properties=connection_props)
    print(f" Loaded {t} : {dfs[t].count()} rows")
  
# C. Add the ETL date 

from pyspark.sql.functions import current_timestamp,current_date # add the time of ETL for each table
for key, df in dfs.items():
    dfs[key] = df.withColumn("dwh_create_date", current_timestamp())

# D. clean table 

#---------------------------------------- 1. ["bronze.crm_cust_info"] ------------------------------#
from pyspark.sql.functions import count
from pyspark.sql.functions import window 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc
from pyspark.sql.functions import when  # Import the when function
from pyspark.sql.window import Window
from pyspark.sql.functions import col, trim
from pyspark.sql.functions import upper

# 1.A. Check duplicate in primary key    *** Spark follows the SQL execution order 

duplicate_key = dfs["bronze.crm_Cust_info"].groupBy("cst_id")\
    .agg(count("*").alias("count")).filter((col("count") > 1))
duplicate_key.show()                                         

# 1.B. explore these duplicates
# dfs["bronze.crm_Cust_info"].filter(col("cst_id") == 29483).orderBy(desc("cst_create_date")).show()
# dfs["bronze.crm_Cust_info"].filter(col("cst_id").isNull()).orderBy(desc("cst_create_date")).show()

# 1.C. drop duplicate, depend on row_number function on the cst_create_date

window_column = Window.partitionBy("cst_id").orderBy(desc("cst_create_date"))      # the newest row

addd_row_number = dfs["bronze.crm_Cust_info"].withColumn("row_number",row_number().over(window_column))
removed_row= addd_row_number.filter(col("row_number")!= 1).show()                        # rows will be deleted
cleaned_cust = addd_row_number.filter((col("row_number") == 1) & (col("cst_id").isNotNull()))  # keep the first row and null key

#cleaned_cust.filter(col("cst_id")== 29466).show()                   # check the duplicate is deleted 

cleaned_cust =cleaned_cust.drop("row_number")                        # drop window column

###--------------------------------------------------------------##
# 1.D. Check for unwanted spaces
# give me the firstname not equal to trimmed_first name (because if equal means no trim happens)
# to detect columns error [cst_firstname]
cleaned_cust_issue = cleaned_cust.filter(col("cst_firstname") != trim(col("cst_firstname")))
cleaned_cust_issue.select("cst_firstname").show()                       ### rows need to trim

cleaned_cust_f_name = cleaned_cust.withColumn("cst_firstname", trim(col("cst_firstname")))

# to detect columns error [cst_lastname]

cleaned_cust_l_name = cleaned_cust_f_name.withColumn("cst_lastname", trim(col("cst_lastname")))

####--------------------------------------------------------------##
# 1.E. Check the gender column ( replace low cardinality ): the m, male, a nd f, female

cleaned_cust_low_card = cleaned_cust_l_name.select("cst_gndr").distinct()

cust_info = cleaned_cust_l_name.withColumn("cst_gndr", when(upper(trim(col("cst_gndr"))) == "M", "Male")\
                                           .when(upper(trim(col("cst_gndr"))) == "F", "Female")\
                                           .otherwise("n/a"))

cleaned_cust_low_card = cust_info.select("cst_gndr").distinct().show()         # for Check

# 1.E. same process in cst_marital_status :
crm_Cust_info= cust_info.withColumn("cst_marital_status",when(upper(trim(col("cst_marital_status")))=="S" ,"Single")\
                                               .when(upper(trim(col("cst_marital_status")))== "M", "Married")\
                                                .otherwise("n/a"))

crm_Cust_info.select("cst_marital_status").distinct().show()                   # for Check

##### cst_create_date
crm_Cust_info.select("cst_create_date")  # for ensure the data type

###------------------------------------------------------------------------------------------------##
#---------------------------------------- 2. ["bronze.crm_cust_info"] ------------------------------#
# dfs["bronze.crm_prd_info"].show(5)                                       #for explore 

# 2.A. find duplicate key  
# dfs["bronze.crm_prd_info"].groupBy("prd_id")\
# .agg(count("*").alias("cnt")).filter((col("cnt") > 1) | (col("prd_id").isNull())).show()  # >>> No result

from pyspark.sql.functions import substring, length, col
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import coalesce, lit
from pyspark.sql.functions import expr

# 2.B. Substring the prod_key to [ Category (5 character) and replace "-"" with "_" ]

silver_prd_cat = dfs["bronze.crm_prd_info"].withColumn("cat_id", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_"))

# 2.C. Substring the prd_key to [ prd_key: from character number 7 to end)

silver_prd_sub = silver_prd_cat.withColumn("prd_key", substring(col("prd_key"), 7, length(col("prd_key"))-6)) #len calculate string lenght

###--------------------------------------------------------------##

# 2.D. check the rows in ["prd_nm"] that needed to trim
# rows_issued = silver_prd_sub.filter(col("prd_nm") != trim(col("prd_nm"))).show()         # No result

# 2.E. check the null and negative value and replace it in ["prd_cost"] column
silver_prd_nulls_cost= silver_prd_sub.filter((col("prd_cost") < 0 ) | (col("prd_cost").isNull()))    ## 2 row  with null

###--------------------------------------------------------------##

# with_fillna_function = silver_prd_sub.fillna({"prd_cost": 0})  # Avoid using .show() here
silver_prd_cleaned_cost = silver_prd_sub.withColumn("prd_cost", coalesce(col("prd_cost"), lit("0")))  # Use coalesce for null handling

# 2.F. check the low-cardinality ["pro_line"] and replace it 
silver_prd_cleaned_cost_line = silver_prd_cleaned_cost.withColumn("prd_line", when(upper(trim(col("prd_line"))) == "R", "Road")\
                                                        .when(upper(trim(col("prd_line"))) == "M", "Mountain")\
                                                        .when(upper(trim(col("prd_line"))) == "S", "Other Sales")\
                                                        .when(upper(trim(col("prd_line"))) == "T", "Tour")\
                                                        .otherwise("n/a"))

# silver_prd_cleaned_cost_line.select("prd_line").distinct().show(10)                       # for Check

# 2.G.Re-arrange the columns
silver_prd =silver_prd_cleaned_cost_line.select("prd_id","cat_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt", "dwh_create_date")

###--------------------------------------------------------------##
        # H.filter the rows with issues in date (end before starting)
from pyspark.sql.functions import lead

# Explore data [count of all rows , null row , problem row]
# print(silver_prd.count())                                           #397
# print(silver_prd.filter(col("prd_end_dt").isNull()).count())        #179
# print(silver_prd.filter(col("prd_end_dt") > col("prd_start_dt")).count()) #0 all rows is problem
# print(silver_prd.filter(col("prd_end_dt") < col("prd_start_dt")).count()) #200 all rows is problem

# H.1. Define the window specification
lead_lag_window = Window.partitionBy("prd_key").orderBy("prd_start_dt")

# H.2. Partition each product and insert a new column with the [next start date]
ordered_silver_pd_lead = silver_prd.withColumn("next_start_date", lead(col("prd_start_dt")).over(lead_lag_window) - 1)

# H.2. Re-arrange the columns
crm_prd_info = ordered_silver_pd_lead.select("prd_id","cat_id","prd_key","prd_nm","prd_cost","prd_line","prd_start_dt",col("next_start_date").alias("prd_end_dt"))

###------------------------------------------------------------------------------------------------##
#---------------------------------------- 3. ["bronze.crm_sales_details"] ------------------------------#
# """
# - The table has a relation with crm_cust_info  ( sls_cust_id ------ Cust_id)
# - the table has a relation with crm_prd_info   ( sls_prd_key ------ prd_key)
# - the data in sales in integer data type and 8 digit 20101229 >> 2010 - 12 - 29 
# ---------- No need for transformation in the first 3 columns ####
# ---------- No need to check for duplicates in the Primary Key because the customer can order more than one product in one order
# """
## --------------------------------A. Explore the product has saled but not in prd_info ------------------------------##
from pyspark.sql.functions import abs
from pyspark.sql.functions import when, col, to_date


sales = dfs["bronze.crm_sales_details"]       # for explore
product = silver_dict["silver.crm_prd_info"]
customer = silver_dict["silver.crm_Cust_info"]
sales.show(2)
z = sales.join(product, sales["sls_prd_key"] ==
               product["prd_key"], "left_anti")
# z.show()                                                # All product in sales has info in crm_prd

## --------------------------------B. explore the customer has saled but not in crm_ cust ------------------------------##
# All product in sales has info in crm_prd
w = sales.join(customer, sales["sls_cust_id"] == customer["cst_id"], "left_anti")
# w.show()                                          # All customer in sales has info in crm_cust
####--------------------------------------------------------------##
# 3.C. Check unwanted spaces

dfs["bronze.crm_sales_details"].filter(col("sls_ord_num")!= trim(col("sls_ord_num"))).show()      # No result


# 3.D. Check Date format in [sls_order_dt]
# sales.printSchema()                            # need to convert column datatype from int to date

# 3.E. Check the data consistency order < ship < due
sales.filter((col("sls_order_dt") > col("sls_ship_dt"))
             | (col("sls_ship_dt") > col("sls_due_dt"))).show()      # date is in place


# E.1 Check if have "0" [mean null but in integer]
# sales.select("sls_cust_id").filter(col("sls_cust_id")==0).show()        # No result
# sales.select("sls_order_dt").filter(col("sls_order_dt")==0).show()      # many zero so need to convert[null]
# sales.select("sls_ship_dt").filter(col("sls_ship_dt")==0).show()         # no result

# 3.F. Check for a bad date or an invalid date
sales.select("sls_order_dt").filter((col("sls_order_dt") <= 0)
                                    | (length(col("sls_order_dt")) != 8)
                                    | (col("sls_order_dt") > 20500101)
                                    | (col("sls_order_dt") < 19000101))

# 3.F. convert 0 with "null" and others to datetime format
sales_date = sales.withColumn("sls_order_dt", when((col("sls_order_dt") <= 0)
                                    | (length(col("sls_order_dt")) != 8), None)
                              .otherwise(to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")))
####--------------------------------------------------------------##

# 3.F. convert Date format in [sls_ship_dt]
sales_date.select("sls_ship_dt").filter((col("sls_ship_dt") <= 0)
                                    | (length(col("sls_ship_dt")) != 8)
                                    | (col("sls_ship_dt") > 20500101)
                                        # No result [cleaned data]
                                    | (col("sls_ship_dt") < 19000101)).show()

sales_ship_date = sales_date.withColumn("sls_ship_dt", to_date(
    col("sls_ship_dt").cast("string"), "yyyyMMdd"))
####--------------------------------------------------------------##


# 3.F. convert Date format in [sls_due_dt]
sales_ship_date.select("sls_due_dt").filter((col("sls_due_dt") <= 0)
                                            | (length(col("sls_due_dt")) != 8)
                                            | (col("sls_due_dt") > 20500101)
                                            # No result [cleaned data]
                                            | (col("sls_due_dt") < 19000101)).show()


sales_due_date = sales_ship_date.withColumn(
    "sls_due_dt", to_date(col("sls_due_dt").cast("string"), "yyyyMMdd"))

####--------------------------------------------------------------##
# 3.G. Transform the remaining column [sls_sales,sls_quantity,sls_price]

"""
#----------------------------------------------------------------------------------------#
# - sls_QYT >>> no zero or negative, so no transform here

Rules :
A. If sales are null or zero or  negative or not equal to [price * QYT ], derive it using QYT * Price
B. If Price is zero or null, calculate using sales and QYT

Note: after division or power, the INT is converted to a Double, so you need (cast) the result
#----------------------------------------------------------------------------------------#
"""
# overview the sls_sales
# sales_due_date.select("*").filter((col("sls_sales").isNull()) | (col("sls_sales")<= 0 )\
# |(abs((col("sls_price")) * col("sls_quantity")) != abs((coalesce(col("sls_sales"),lit(0)))))).show()

# check if have null in sls_sales & price with same row
# sales_due_date.select("*").filter((col("sls_sales").isNull()) & (col("sls_price").isNull())).show() # No result


sales_price_sales = sales_due_date.withColumn("sls_sales", when(
    (col("sls_sales").isNull())
    | (col("sls_sales") <= 0)
    | ((col("sls_price").isNotNull()) & ((abs(col("sls_price")) * col("sls_quantity")) != col("sls_sales"))), (abs(coalesce(col("sls_price"), lit(0))) * col("sls_quantity")).cast("int"))
    .otherwise(col("sls_sales")))


# Quality check for sls_sales
sales_price_sales.select("*").filter((col("sls_sales").isNull())
                                     # No result
                                     | (col("sls_sales") <= 0)).show()

# -----------------------------------------------------------------------------------------------------#
#  3.G. calculate [sales_price]
# overview the sls_sales
# sales_price_sales.select("*").filter((col("sls_price").isNull()) | (col("sls_price")< 0 )).distinct().show()

crm_sales_details = sales_price_sales.withColumn("sls_price", when(
    (col("sls_price").isNull())
    | (col("sls_price") <= 0), (col("sls_sales") / col("sls_quantity")).cast("int"))
    .otherwise(col("sls_price")))


###------------------------------------------------------------------------------------------------##
#---------------------------------------- 4. ["bronze.erp_cust_az12"] ------------------------------#

# """
# - the table has relation with crm_cust_info  (CID   ----- cst_key)

# 1. found relation between erp_cust_az12 & crm_Cust_info when substring from character 
#      3 of ERP [CID]
# 2. When do left joins appear? CID no need for substring, so need when condition [LIKE operator]
# 3. Check the date, if the date > current date, the best thing leave it with null
# 4. deal with missing values in gender & unwanted spaces & low-cardinality 
# """

## --------------------------------A. explore the product has saled but not in prd_info ------------------------------##

dfs["bronze.erp_cust_az12"]
crm_cust = silver_dict["silver.crm_Cust_info"]
erp_cust = dfs["bronze.erp_cust_az12"]

# crm_cust.select("cst_key").distinct().show()                           # All records started with AW
                                # A. Substring Word started NAS / for starting AW > No change
                                # due all CRM Cust Id started with AW
erp_cust_id = erp_cust.withColumn("CID", when (
                   col("CID").like("NAS%"),substring(col("CID"),4,length(col("CID"))-3))\
                   .otherwise(col("CID")))

# # erp_cust_id.select("c_id").distinct().show()
# erp_cust_id.join(crm_cust, crm_cust["cst_key"]==erp_cust_id["CID"], "left_anti").show()   # no result [done] 

# -------------------------------------------------------------------------------#
                               # B. Check birthday

# erp_cust_id.select("*").filter(col("BDATE") > current_date()).show() # not logical true
erp_cust_date =erp_cust_id.withColumn("BDATE",when(
                                   (col("BDATE") > current_date()), None)
                                   .otherwise(col("BDATE")))
# Quality check
# erp_cust_date.select("*").filter(col("BDATE").isNull()).show()  # null

# -------------------------------------------------------------------------------#
                               # D. Gender
erp_cust_date.select("GEN").distinct().show()   # unwanted spaced / low cardinality/nulls

erp_cust_az12 = erp_cust_date.withColumn("GEN", when(
                                         upper(trim(col("GEN"))) == "F" , "Female")
                                        .when(upper(trim(col("GEN"))) == "M", "Male")
                                        .when((trim(col("GEN")) == "")\
                                        |(col("GEN").isNull()), "n/a")
                                        .otherwise(trim("GEN")))


erp_cust_az12.select("GEN").distinct().show()   # unwanted spaced / low cardinality / nulls


###------------------------------------------------------------------------------------------------##
#---------------------------------------- 5. ["bronze.erp_loc_a101"] ------------------------------#

# """
# - the table has relation with crm_cust_info  ( cid ------ cst_key)
# - the table has cntry column
# 1. the CID in erp_loc has ("-") after AW and this is not acceptable so we need replace with ""
# 2. check low cardinality in cntry
# """
## --------------------------------A. explore the product has saled but not in prd_info ------------------------------##

dfs["bronze.erp_cust_az12"]
crm_cust_2 = silver_dict["silver.crm_Cust_info"]
erp_loc = dfs["bronze.erp_loc_a101"]

# crm_cust_2.show(2)
# erp_loc.show(2)

erp_loc_10 = erp_loc.withColumn("CID", regexp_replace(col("CID"), "-", ""))
# erp_loc_10.show(2)

#Quality check [expected No Result]
# erp_loc_10.join(crm_cust_2, erp_loc_10["CID"]== crm_cust_2["cst_key"],"left_anti").show()
erp_loc_a101= erp_loc_10.withColumn("CNTRY", when
                                    ((upper(trim(col("CNTRY")))=="US")\
                                    |(upper(trim(col("CNTRY")))=="USA")\
                                    ,"United States")\
                                    .when(((col("CNTRY").isNull()))\
                                    |(trim(col("CNTRY"))== ""),"n/a")
                                    .when((trim(col("CNTRY"))=="DE"),"Germany")
                                    .otherwise(trim(col("CNTRY"))))
                                    
erp_loc_a101.select("CNTRY").distinct().show()

###------------------------------------------------------------------------------------------------##
#---------------------------------------- 6. [bronze.erp_px_cat_g1v2] ------------------------------#
# """
# - the table has relation with crm_prd_info  ( ID ------ cat_id)
# ** we have product with none information in erp_px_cat table [ID = "CO_PE"]
# ** we have category with none product in crm_prd_info table [ID = "CO_PD"]

# - the table has product info [Category, Sub category, Maintenance]
# """
## --------------------------------A. explore the relation between two table ------------------------------##

erp_px_cat_g1v2 = dfs["bronze.erp_px_cat_g1v2"]
y = silver_dict["silver.crm_Cust_info"]

# x.join(y ,y["cat_id"]== x["ID"], "left_anti").select("ID").distinct().show()    # one id
# y.join(x ,y["cat_id"]== x["ID"], "left_anti").select("cat_id").distinct().show() # one ID

# x.show(5)
## --------------------------------B. CAT Column ------------------------------##
x.select("CAT").distinct().show(20)                                             # 4 distinct CAT 
x.select("SUBCAT").distinct().show()                                             # 4 distinct CAT 
x.filter((col("CAT")!= trim(col("CAT")))\
        |(col("CAT")!= trim(col("CAT")))\
        |(col("MAINTENANCE")!= trim(col("MAINTENANCE")))
        ).show()                                                            # no need for trim in all Column


x.select("MAINTENANCE").distinct().show()                                       # 2 result [yes , no]
x.filter(col("CAT")!= trim(col("CAT"))).show()                                  # no result > no need trim
x.filter(col("MAINTENANCE")!= trim(col("MAINTENANCE"))).show()                  # no result > no need trim


## -------------------------------- the table is clean  ------------------------------##

                            # Assign the table into silver dict
silver_dict["silver.erp_px_cat_g1v2"] = erp_px_cat_g1v2
print(silver_dict.keys())



