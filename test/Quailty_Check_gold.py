"""
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks before or after data loading Silver Layer: using {silver_dict}
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
"""

#----------------------------- #### Quality check  of Customer_Dim ------------------------------#
"""
                        1.    Quality Check 
"""
####--------------------------------------------------------------##

# Check if we have duplicates in customer info [ Expected No result]

customer_dim.groupBy("customer_number").agg(count("*").alias("c"))\
    .filter(col("c")>1).show()

# Check is_current  [ Expected No result]

customer_dim.filter((col("end_date").isNotNull())\
                    & (col("is_current")== 0)).show()
                    

customer_dim.filter((col("end_date").isNull())\
                    & (col("is_current")== 0)).show()

# Check Gender  [ Expected Male - Female - n/a]

customer_dim.select("Gender").distinct().show()

# Check Data_Types
customer_dim.printSchema()

# Check SK  [ Expected 18484, equal count rows]

print(customer_dim.count())                                     #18484
customer_dim.agg(max("cust_Sk")).show()                         #18484

#----------------------------- #### Quality check  of Product_Dim ------------------------------#
"""
                      2.      Quality Check 
"""
####--------------------------------------------------------------##

# Check Data_Types
customer_dim.printSchema()

# Show the product with null category [Expected 7 rows Null in 4 column]
product_dim.filter(col("Category").isNull()).show()

# Show the Category with null produced [Expected 1 row Null in 6]           # if use Full_Join
product_dim.filter(col("prd_id").isNull()).show()

# Check is_current column [Expected: No result ]
product_dim.filter((col("start_dt").isNotNull())& (col("end_dt").isNotNull()))\
    .filter(col("Is_Current")==1).show()          # No result

#----------------------------- #### Quality check  of Fact_sales ------------------------------#
"""
                     3.       Quality Check 
"""
####--------------------------------------------------------------##

# 1- Fact Surggote Key [Expected the same number]
fact_sales.agg(max("Sales_Sk")).show()           # 60398                
print(fact_sales.count())                        # 60398

# 2- Dimensions SK [Expected customer with many orders] ex: Customer_sk =  26 
fact_sales.groupBy("Customer_FK").agg(count("*").alias("c")).filter(col("c")>1).limit(1).show()

# 3- Customer Dimensions SK = 26 [Expected 6 rows]
fact_sales.alias("f").join(customer_dim, fact_sales["Customer_FK"]== customer_dim["cust_Sk"],"inner")\
.filter(col("cust_Sk")== 26).select("f.*").show()

# 4- Test the Customer sales [Expected 6577]
fact_sales.alias("f").join(customer_dim, fact_sales["Customer_FK"]== customer_dim["cust_Sk"],"inner")\
.filter(col("cust_Sk")== 26).agg(sum("Total_sales")).show()          # 6577

# 5- Product Dimensions with high purchase [Expected Product_fk = 3, 4244 ]
fact_sales.alias("f").join(product_dim, fact_sales["Product_FK"]== product_dim["Product_SK"],"inner")\
.groupby("f.Product_FK").agg(count("Total_sales").alias("Hights_product")).orderBy(col("Hights_product").asc()).show()

# 6- Tracking the CUSTOMER with no order [Expected No Result]
fact_sales.alias("f").join(customer_dim.alias("c"), fact_sales["Customer_FK"]== customer_dim["cust_Sk"],  "left")\
    .join(product_dim.alias("p"), fact_sales["Product_FK"]== product_dim["Product_SK"], "left")\
    .filter((col("c.cust_Sk").isNull())|(col("p.Product_SK").isNull())).show()

