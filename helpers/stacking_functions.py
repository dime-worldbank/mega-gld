"""Core data processing functions for GLD stacking pipeline."""

from pyspark.sql.functions import col, greatest, lit, coalesce


def identify_changes(metadata_df):
    """
    Identify tables that need to be updated based on version changes.
    
    Args:
        metadata_df: DataFrame containing ingestion metadata
        
    Returns:
        DataFrame with tables that have newer versions
    """
    # Treat null as -1 (not yet in harmonized table)
    stacked_all_val = coalesce(col("stacked_all_table_version"), lit(-1))
    stacked_ouo_val = coalesce(col("stacked_ouo_table_version"), lit(-1))
    
    # Filter tables where source version is newer than stack version
    change_keys = metadata_df.filter(
        col("table_version") > greatest(stacked_all_val, stacked_ouo_val)
    ).select(
        "table_name",
        "classification",
        col("country").alias("countrycode"),
        "year",
        "survey",
        "table_version",
        "stacked_all_table_version",
        "stacked_ouo_table_version"
    ).distinct()
    
    return change_keys


def build_update_list(change_keys_df):
    """
    Build a list of updates from the change keys DataFrame.
    
    Args:
        change_keys_df: DataFrame with tables that need updates
        
    Returns:
        list: List of tuples (table_name, classification, country, year, survey)
    """
    update_list = []
    
    for row in change_keys_df.collect():
        table_name = row["table_name"]
        classification = row["classification"]
        country = row["countrycode"]
        year = row["year"]
        survey = row["survey"]
        table_version = row["table_version"]
        
        update_list.append((table_name, classification, country, year, survey))
        
        # Log the action
        if row["stacked_all_table_version"] is None:
            print(f"ACTION: Adding BRAND NEW data for {country} {year} {survey} "
                  f"of the latest version {table_version}")
        else:
            print(f"ACTION: UPDATING existing data for {country} {year} {survey} "
                  f"with the latest version {table_version} (Newer version detected)")
    
    return update_list


def align_dataframe_to_schema(src_df, schema, country_val, survey_val):
    """
    Align a source DataFrame to the standard schema.
    
    Args:
        src_df: Source DataFrame to align
        schema: Target schema (StructType)
        country_val: Country code value to add
        survey_val: Survey name value to add
        
    Returns:
        tuple: (aligned DataFrame, list of extra columns ignored)
    """
    from helpers.stacking_schema import is_dynamic_column
    
    # Extract expected columns from schema
    expected_cols = [f.name for f in schema.fields]
    
    selected_exprs = []
    
    for c in expected_cols:
        if c == "countrycode" and country_val is not None:
            selected_exprs.append(lit(country_val).cast(schema[c].dataType).alias(c))
        elif c == "survname" and survey_val is not None:
            selected_exprs.append(lit(survey_val).cast(schema[c].dataType).alias(c))
        elif c in src_df.columns:
            # Column exists in source, cast it to schema type
            selected_exprs.append(col(c).cast(schema[c].dataType).alias(c))
        else:
            # Column is missing in source, fill with NULL
            selected_exprs.append(lit(None).cast(schema[c].dataType).alias(c))
    
    # Add dynamic columns (subnational and GAUL)
    dynamic_cols = [c for c in src_df.columns if is_dynamic_column(c)]
    for dc in dynamic_cols:
        if dc not in expected_cols:
            selected_exprs.append(col(dc).cast("string").alias(dc))
    
    # Identify extra columns that will be ignored
    extra_cols = [
        c for c in src_df.columns
        if c not in expected_cols and c not in dynamic_cols
    ]
    
    # Create the aligned DataFrame
    aligned_df = src_df.select(*selected_exprs)
    
    return aligned_df, extra_cols



def update_metadata_versions(metadata_df, change_keys_df):
    """
    Update metadata with new stacked versions.
    
    Args:
        metadata_df: Original metadata DataFrame
        change_keys_df: DataFrame with tables that need updates (includes table_version)
        
    Returns:
        DataFrame: Updated metadata DataFrame
    """
    # Extract updates from change_keys
    updates_df = change_keys_df.select(
        col("countrycode").alias("country"),
        "year",
        "survey",
        col("table_version").alias("new_version").cast("int")
    )
    
    # Join with original metadata
    metadata_updated_df = metadata_df.join(
        updates_df,
        on=["country", "year", "survey"],
        how="left"
    )
    
    # Update version columns
    metadata_final = metadata_updated_df.withColumn(
        "stacked_all_table_version",
        coalesce(col("new_version"), col("stacked_all_table_version"))
    ).withColumn(
        "stacked_ouo_table_version",
        coalesce(col("new_version"), col("stacked_ouo_table_version"))
    ).drop("new_version")
    
    return metadata_final
