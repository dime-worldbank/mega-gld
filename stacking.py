# Databricks notebook source
from helpers.stacking_schema import get_gld_schema
from helpers.stacking_functions import (
    identify_changes,
    build_update_list,
    align_dataframe_to_schema,
    update_metadata_versions
)

# COMMAND ----------

TABLE_QULIFIER = "prd_csc_mega.sgld48"
OFFICIAL_CLASS = "Official Use"

# TODO: add this to the metadata table as a flag
TO_REMOVE = ['MEX_2023_ENOE_Panel_V01_M_V01_A_GLD_ALL', 'IND_2022_PLFS_Urban_Panel_V01_M_V01_A_GLD_ALL']

# Table names
METADATA_TABLE = f"{TABLE_QULIFIER}.test_ingestion_metadata"
HARMONIZED_CONFIDENTIAL = f"{TABLE_QULIFIER}.test_GLD_HARMONIZED_ALL"
HARMONIZED_OFFICIAL = f"{TABLE_QULIFIER}.test_GLD_HARMONIZED_OUO"

# COMMAND ----------

schema = get_gld_schema()
expected_cols = [f.name for f in schema.fields]

# COMMAND ----------

# Identify which country/year/survey to update
metadata = spark.table(METADATA_TABLE)
change_keys = identify_changes(metadata)


# COMMAND ----------


# Get or create existing harmonized tables
if spark.catalog.tableExists(HARMONIZED_CONFIDENTIAL):
    harmonized_all = spark.table(HARMONIZED_CONFIDENTIAL)
else:
    harmonized_all = spark.createDataFrame([], schema)

if spark.catalog.tableExists(HARMONIZED_OFFICIAL):
    harmonized_ouo = spark.table(HARMONIZED_OFFICIAL)
else:
    harmonized_ouo = spark.createDataFrame([], schema)

# Remove records that will be updated
harmonized_all_cleaned = harmonized_all.join(change_keys, on=["countrycode", "year", "survey"], how="left_anti")
harmonized_ouo_cleaned = harmonized_ouo.join(change_keys, on=["countrycode", "year", "survey"], how="left_anti")


# COMMAND ----------

# Process all tables
from helpers.stacking_schema import is_dynamic_column
from pyspark.sql.functions import col, lit
from functools import reduce

all_dfs = []
ouo_dfs = []
update_list = build_update_list(change_keys)

for tbl, classification, country_val, year_val, survey_val in update_list:
    # Skip excluded tables
    if tbl in TO_REMOVE:
        print(f"Skipping excluded table: {tbl}")
        continue

    # Read source table
    src_df = spark.table(f"{TABLE_QULIFIER}.{tbl}")
    
    # Align to schema  
    aligned_df, extra_cols = align_dataframe_to_schema(
        src_df, schema, country_val, survey_val
    )
    
    # Log extra columns
    if extra_cols:
        print(f"Extra columns ignored: {extra_cols} for {country_val} {year_val}")
    
    # Add to appropriate lists
    all_dfs.append(aligned_df)
    if classification == OFFICIAL_CLASS:
        ouo_dfs.append(aligned_df)

# COMMAND ----------

# Union all tables and write results
if all_dfs:
    final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_dfs)
    final_df = harmonized_all_cleaned.unionByName(final_df, allowMissingColumns=True)
    final_df.write.mode("overwrite").saveAsTable(HARMONIZED_CONFIDENTIAL)

if ouo_dfs:
    ouo_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), ouo_dfs)
    ouo_df = harmonized_ouo_cleaned.unionByName(ouo_df, allowMissingColumns=True)
    ouo_df.write.mode("overwrite").saveAsTable(HARMONIZED_OFFICIAL)

# COMMAND ----------

# Update metadata with new stacked versions
metadata_final = update_metadata_versions(metadata, change_keys)
metadata_final.write.mode("overwrite").saveAsTable(METADATA_TABLE)
