# Databricks notebook source
#TESTING
import pyspark.sql.functions as f



import pyspark.sql.functions as f

# 1. Read the source table
# 2. Randomize using f.rand()
# 3. Limit to 20 rows
test_df = spark.table("`prd_csc_mega`.`sgld48`.`_ingestion_metadata`") \
    .orderBy(f.rand()) \
    .limit(20)

# 4. Save as the new test table
# 'overwrite' mode mimics 'CREATE OR REPLACE'
test_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("`prd_csc_mega`.`sgld48`.`test_ingestion_metadata`")
    
# 1. Grab 5 random table names and "freeze" them into a Python list
# .collect() pulls the data out of the Spark plan and into local memory
random_rows = spark.table("`prd_csc_mega`.`sgld48`.`test_ingestion_metadata`") \
    .select("table_name") \
    .orderBy(f.rand()) \
    .limit(5) \
    .collect()

# Create a list of strings: ['table_a', 'table_b', ...]
target_tables = [row.table_name for row in random_rows]

# 2. Update the Delta table using the fixed list
# This uses the standard SQL 'IN' syntax but fills it with our Python list
if target_tables:
    names_tuple = str(tuple(target_tables)).replace(",)", ")") # Handle single-item edge case
    
    spark.sql(f"""
        UPDATE `prd_csc_mega`.`sgld48`.`test_ingestion_metadata`
        SET stacked_ouo_table_version = stacked_ouo_table_version + 1,
            stacked_all_table_version = stacked_all_table_version + 1
        WHERE table_name IN {names_tuple}
    """)
    
    print(f"Successfully updated: {target_tables}")
else:
    print("No rows found to update.")
