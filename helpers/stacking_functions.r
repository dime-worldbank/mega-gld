# Core data processing functions for GLD stacking pipeline

library(dplyr)
library(sparklyr)


#' Identify tables that need to be updated based on version changes
#'
#' @param metadata_df DataFrame containing ingestion metadata
#' @return DataFrame with tables that have newer versions
identify_changes <- function(metadata_df) {
  # Treat null as -1 (not yet in harmonized table)
  change_keys <- metadata_df %>%
    mutate(
      stacked_all_val = coalesce(stacked_all_table_version, -1),
      stacked_ouo_val = coalesce(stacked_ouo_table_version, -1),
      max_stacked_version = greatest(stacked_all_val, stacked_ouo_val)
    ) %>%
    filter(table_version > max_stacked_version) %>%
    select(
      table_name,
      classification,
      countrycode = country,
      year,
      survname = survey,
      table_version,
      stacked_all_table_version,
      stacked_ouo_table_version
    ) %>%
    # EXPLICIT CASTING TO MATCH TARGET TABLE
    mutate(
      year = as.integer(year),
      countrycode = as.character(countrycode),
      survname = as.character(survname)
    ) %>%
    distinct()
  
  return(change_keys)
}


#' Build a list of updates from the change keys DataFrame
#'
#' @param change_keys_df DataFrame with tables that need updates
#' @return List of lists, each containing: table_name, classification, country, year, survey
build_update_list <- function(change_keys_df) {
  # Collect the data
  change_data <- change_keys_df %>% collect()
  
  update_list <- list()
  
  for (i in seq_len(nrow(change_data))) {
    row <- change_data[i, ]
    
    table_name <- row$table_name
    classification <- row$classification
    country <- row$countrycode
    year <- row$year
    survname <- row$survname
    table_version <- row$table_version
    
    # Add to update list
    update_list[[i]] <- list(
      table_name = table_name,
      classification = classification,
      country = country,
      year = year,
      survname = survname
    )
    
    # Log the action
    if (is.na(row$stacked_all_table_version)) {
      message(sprintf(
        "ACTION: Adding BRAND NEW data for %s %s %s of the latest version %s",
        country, year, survname, table_version
      ))
    } else {
      message(sprintf(
        "ACTION: UPDATING existing data for %s %s %s with the latest version %s (Newer version detected)",
        country, year, survname, table_version
      ))
    }
  }
  
  return(update_list)
}


#' Align a source DataFrame to the standard schema
#'
#' @param src_df Source DataFrame to align
#' @param schema Target schema (named list with column names and types)
#' @param country_val Country code value to add
#' @param survey_val Survey name value to add
#' @return List with two elements: aligned_df (DataFrame) and extra_cols (character vector)
align_dataframe_to_schema <- function(src_df, schema, country_val, survey_val) {
  source("helpers/stacking_schema.r")
  
  # Extract expected columns from schema
  expected_cols <- names(schema)
  
  # Get source columns
  src_cols <- colnames(src_df)
  
  # Build mutate expressions for expected columns
  mutate_exprs <- list()
  
  for (col_name in expected_cols) {
    target_type <- schema[[col_name]]
    
    if (col_name == "countrycode" && !is.null(country_val)) {
      mutate_exprs[[col_name]] <- sql(paste0("CAST('", country_val, "' AS ", target_type, ")"))
    } else if (col_name == "survname" && !is.null(survey_val)) {
      mutate_exprs[[col_name]] <- sql(paste0("CAST('", survey_val, "' AS ", target_type, ")"))
    } else if (col_name %in% src_cols) {
      # Column exists in source, cast it to schema type
      mutate_exprs[[col_name]] <- sql(paste0("CAST(", col_name, " AS ", target_type, ")"))
    } else {
      # Column is missing in source, fill with NULL
      mutate_exprs[[col_name]] <- sql(paste0("CAST(NULL AS ", target_type, ")"))
    }
  }
  
  # Identify dynamic columns (subnational and GAUL)
  dynamic_cols <- src_cols[sapply(src_cols, is_dynamic_column)]
  dynamic_cols <- setdiff(dynamic_cols, expected_cols)
  
  # Add dynamic columns to mutate expressions
  for (dc in dynamic_cols) {
    mutate_exprs[[dc]] <- sql(paste0("CAST(", dc, " AS string)"))
  }
  
  # Final column list
  final_column_names <- c(expected_cols, dynamic_cols)
  
  # Identify extra columns that will be ignored
  extra_cols <- setdiff(src_cols, final_column_names)
  
  # Apply transformation
  aligned_df <- src_df %>%
    mutate(!!!mutate_exprs) %>%
    select(all_of(final_column_names))
  
  return(list(
    aligned_df = aligned_df,
    extra_cols = extra_cols
  ))
}


#' Update metadata with new stacked versions
#'
#' @param metadata_df Original metadata DataFrame
#' @param change_keys_df DataFrame with tables that need updates (includes table_version)
#' @return Updated metadata DataFrame
update_metadata_versions <- function(metadata_df, change_keys_df) {
  # Extract updates from change_keys
  updates_df <- change_keys_df %>%
    select(
      country = countrycode,
      year,
      survey = survname,
      new_version = table_version
    ) %>%
    mutate(new_version = as.integer(new_version))
  
  # Join with original metadata
  metadata_updated_df <- metadata_df %>%
    left_join(updates_df, by = c("country", "year", "survey"))
  
  # Update version columns using coalesce
  metadata_final <- metadata_updated_df %>%
    mutate(
      stacked_all_table_version = coalesce(new_version, stacked_all_table_version),
      stacked_ouo_table_version = coalesce(new_version, stacked_ouo_table_version)
    ) %>%
    select(-new_version)
  
  return(metadata_final)
}
