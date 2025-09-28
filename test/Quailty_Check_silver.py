#----------------------------- #### Quality check  of silver crm cust info ------------------------------#
"""
                        1.    Quality Check 
"""
####--------------------------------------------------------------##

silver_dict = {}
silver_dict["silver.crm_Cust_info"] = crm_Cust_info

# # Check the duplicate [Expected: No result ]
silver_dict["silver.crm_Cust_info"].groupBy("cst_id")\
.agg(count("*").alias("cnt")).filter(col("cnt") > 1).show()       #  No result

# to detect column errors [Expected: No result ]
silver_dict["silver.crm_Cust_info"].filter(col("cst_firstname") != trim(col("cst_firstname"))).show() # >>> No result
silver_dict["silver.crm_Cust_info"].filter(col("cst_lastname") != trim(col("cst_lastname"))).show() # >>> No result

# detect convert low_cardinality [Expected: No Cardinality column ]
silver_dict["silver.crm_Cust_info"].select("cst_gndr").distinct().show()
silver_dict["silver.crm_Cust_info"].select("cst_marital_status").distinct().show()


#----------------------------- #### Quality check  of silver crm prd info ------------------------------#
"""
                      2.      Quality Check 
"""
####--------------------------------------------------------------##

 # Assign the table into silver dict
silver_dict["silver.crm_prd_info"] = crm_prd_info
print(silver_dict.keys())

# check duplicate in primary key [Expected No Result]
silver_dict["silver.crm_prd_info"].groupBy("prd_id")\
.agg(count("*").alias("cnt")).filter(col("cnt") > 1).show() # >>> No result 

# Check for unwanted spaces [Expected No Result]
silver_dict["silver.crm_prd_info"].filter(col("prd_nm") != trim(col("prd_nm"))).show() # >>> No result 

# Check for Nulls or Negative Numbers [Expected No Result]
silver_dict["silver.crm_prd_info"].filter((col("prd_cost") < 0) | (col("prd_cost").isNull())).show()

# Check for Data Standardization $ Consistency [Expected No low-cardinality character]
silver_dict["silver_crm_prd_info"].select("prd_line").distinct().show()

# Check Invalid * Date * Order [expected No Result - No End < Start Date]
silver_dict["silver.crm_prd_info"].select("prd_start_dt","prd_end_dt").filter((col("prd_start_dt") > col("prd_end_dt")) \
                                                                              & col("prd_end_dt").isNotNull()).show()
# overview
silver_dict["silver.crm_prd_info"].show()

#----------------------------- #### Quality check  of silver crm sales details ------------------------------#
"""
                     3.       Quality Check 
"""
####--------------------------------------------------------------##
# check  Null, negative, or zero  [Expected No Result]
crm_sales_details.filter((col("sls_order_dt") > col("sls_ship_dt"))\
                                    | (col("sls_ship_dt")> col("sls_due_dt"))).show()

# check  Null, negative, or zero  [Expected No Result]
crm_sales_details.select("*").filter((col("sls_sales") <= 0) | (col("sls_sales").isNull())).show() 

# check Null, negative, or zero  [Expected No Result]
crm_sales_details.select("*").filter((col("sls_price") <= 0) | (col("sls_price").isNull())).show() 

# check the error in calculation  [Expected No Result]
crm_sales_details.select("*").filter((col("sls_price") * col("sls_quantity")) != col("sls_sales")).show() 

crm_sales_details.printSchema()

                    # H.Assign the table into the silver dict

silver_dict["silver.crm_sales_details"]= crm_sales_details 
print(silver_dict.keys())

#-------------------------- #### Quality check  of silver Erp cust az12 ----------------------------#
"""
                   4.         Quality Check 
"""
####--------------------------------------------------------------##

# check the all ID is in erp_cust is correct with crm_cust [Expected No Result]
erp_cust_az12.join(crm_cust, crm_cust["cst_key"]== erp_cust_az12["CID"], "left_anti").show() # no result [done] 

# check the bdate is logic [!> current date] [Expected no ]
erp_cust_az12.select("*").filter((col("BDATE") > current_date())).show()  # No result


# check Gender is not null, trimmed, high-cardinality  [Expected Male, Female, N/a]

erp_cust_az12.select("GEN").distinct().show()  

                       # H.Assign the table into the silver dict

silver_dict["silver.erp_cust_az12"]= erp_cust_az12 
print(silver_dict.keys())

#-------------------------- #### Quality check  of bronze erp loc a101"] ----------------------------#
"""
                 5.           Quality Check 
"""
####--------------------------------------------------------------##
# Quality Check of silver erp _ loc

# check the all ID is in erp_loc is correct with crm_cust [expected No Result]
erp_loc_a101.join(crm_cust_2, erp_loc_a101["CID"]== crm_cust_2["cst_key"],"left_anti").show()



# check Gender is not null, trimed , high-cardinality  [expected No Null or duplicate in meaning]

erp_loc_a101.select("CNTRY").distinct().show()  

                       # H.Assign the table into silver dict

silver_dict["silver.erp_loc_a101"]= erp_loc_a101
print(silver_dict.keys())


