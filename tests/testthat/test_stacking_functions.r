# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(sparklyr)
  library(dplyr)
})

# COMMAND ----------

source("../../helpers/stacking_functions.r")
source("../../helpers/stacking_schema.r")

# COMMAND ----------

# Setup Spark connection for tests
sc <- spark_connect(method = "databricks")

# Define the constant used inside identify_changes
OFFICIAL_CLASS <- "Official Use"

# Helper: temporarily replace get_delta_table_version with a stub that
# returns a fixed version number, then restore the original on exit.
with_mocked_delta_version <- function(version, expr) {
  original <- get("get_delta_table_version", envir = .GlobalEnv)
  assign("get_delta_table_version",
         function(table_name, sc) as.integer(version),
         envir = .GlobalEnv)
  on.exit(assign("get_delta_table_version", original, envir = .GlobalEnv))
  force(expr)
}

# COMMAND ----------

# =============================================================================
# Test identify_changes function
# =============================================================================

test_that("identify_changes detects new tables (NULL stacked_all_table_version)", {
  # Case 1: stacked_all_table_version is NA => table has never been stacked
  test_metadata <- copy_to(sc, data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    table_version = 1,
    stacking = 1,
    stacked_all_table_version = NA_integer_,
    stacked_ouo_table_version = NA_integer_,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  changes <- identify_changes(test_metadata) %>% collect()

  expect_equal(nrow(changes), 1)
  expect_equal(changes$table_name, "TEST_2020_LFS_V01_M_V01_A_GLD_ALL")
  expect_equal(changes$countrycode, "TST")
  expect_equal(changes$year, 2020L)
  expect_equal(changes$survname, "LFS")
  expect_equal(changes$table_version, 1)
})

test_that("identify_changes detects Official Use tables with missing OUO version", {
  # Case 2: stacked_all is already set, but stacked_ouo is NA for an Official
  # Use survey => the OUO harmonised table still needs updating.
  test_metadata <- copy_to(sc, data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    table_version = 3,
    stacking = 1,
    stacked_all_table_version = 3,          # ALL is current
    stacked_ouo_table_version = NA_integer_, # OUO is missing
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  changes <- identify_changes(test_metadata) %>% collect()

  expect_equal(nrow(changes), 1)
  expect_equal(changes$table_name, "TEST_2020_LFS_V01_M_V01_A_GLD_ALL")
})

test_that("identify_changes ignores fully up-to-date tables", {
  # Both stacked versions match => nothing to do
  test_metadata <- copy_to(sc, data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    table_version = 2,
    stacking = 1,
    stacked_all_table_version = 2,
    stacked_ouo_table_version = 2,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  changes <- identify_changes(test_metadata) %>% collect()

  expect_equal(nrow(changes), 0)
})

test_that("identify_changes excludes tables with stacking = 0", {
  # Rows flagged stacking = 0 must never be processed, even if versions are NA
  test_metadata <- copy_to(sc, data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    table_version = 1,
    stacking = 0,
    stacked_all_table_version = NA_integer_,
    stacked_ouo_table_version = NA_integer_,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  changes <- identify_changes(test_metadata) %>% collect()

  expect_equal(nrow(changes), 0)
})

test_that("identify_changes handles both Case 1 and Case 2 simultaneously", {
  # Row 1 (2020): ALL is set, OUO is NA, Official Use => Case 2 fires
  # Row 2 (2021): ALL is NA => Case 1 fires
  test_metadata <- copy_to(sc, data.frame(
    table_name = c(
      "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
      "TEST_2021_LFS_V01_M_V01_A_GLD_ALL"
    ),
    classification = c("Official Use", "Official Use"),
    country = c("TST", "TST"),
    year = c("2020", "2021"),
    survey = c("LFS", "LFS"),
    table_version = c(3, 3),
    stacking = c(1, 1),
    stacked_all_table_version = c(2L, NA_integer_),
    stacked_ouo_table_version = c(NA_integer_, NA_integer_),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  changes <- identify_changes(test_metadata) %>% collect()

  expect_equal(nrow(changes), 2)
})

# COMMAND ----------

# =============================================================================
# Test build_update_list function
# =============================================================================

test_that("build_update_list creates proper list structure", {
  test_changes <- copy_to(sc, data.frame(
    table_name = c(
      "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
      "TEST_2021_LFS_V01_M_V01_A_GLD_ALL"
    ),
    classification = c("Official Use", "Confidential"),
    countrycode = c("TST", "TST"),
    year = c(2020L, 2021L),
    survname = c("LFS", "LFS"),
    table_version = c(1, 2),
    stacked_all_table_version = c(NA_integer_, 1),
    stacked_ouo_table_version = c(NA_integer_, 1),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  update_list <- build_update_list(test_changes)

  expect_equal(length(update_list), 2)

  # Check first item
  expect_equal(update_list[[1]]$table_name, "TEST_2020_LFS_V01_M_V01_A_GLD_ALL")
  expect_equal(update_list[[1]]$classification, "Official Use")
  expect_equal(update_list[[1]]$country, "TST")
  expect_equal(update_list[[1]]$year, 2020)
  expect_equal(update_list[[1]]$survname, "LFS")

  # Check second item
  expect_equal(update_list[[2]]$table_name, "TEST_2021_LFS_V01_M_V01_A_GLD_ALL")
})

test_that("build_update_list handles empty input", {
  test_changes <- copy_to(sc, data.frame(
    table_name = character(0),
    classification = character(0),
    countrycode = character(0),
    year = integer(0),
    survname = character(0),
    table_version = integer(0),
    stacked_all_table_version = integer(0),
    stacked_ouo_table_version = integer(0),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  update_list <- build_update_list(test_changes)

  expect_equal(length(update_list), 0)
})

# COMMAND ----------

# =============================================================================
# Test align_dataframe_to_schema function
# =============================================================================

test_that("align_dataframe_to_schema adds countrycode and survname", {
  schema <- get_gld_schema()

  test_df <- copy_to(sc, data.frame(
    year = 2020L,
    hhid = "001",
    pid = "001-01",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS")
  aligned_df <- result$aligned_df %>% collect()

  expect_equal(aligned_df$countrycode[1], "TST")
  expect_equal(aligned_df$survname[1], "LFS")
})

test_that("align_dataframe_to_schema fills missing columns with NULL", {
  schema <- get_gld_schema()

  test_df <- copy_to(sc, data.frame(
    hhid = "001",
    pid = "001-01",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS")
  aligned_df <- result$aligned_df %>% collect()

  # All schema columns must be present
  schema_cols <- names(schema)
  expect_true(all(schema_cols %in% names(aligned_df)))

  # Columns absent from the source should be NA
  expect_true(is.na(aligned_df$lstatus[1]))
  expect_true(is.na(aligned_df$empstat[1]))
})

test_that("align_dataframe_to_schema identifies extra columns", {
  schema <- get_gld_schema()

  test_df <- copy_to(sc, data.frame(
    hhid = "001",
    pid = "001-01",
    extra_col1 = "value1",
    extra_col2 = "value2",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS")
  extra_cols <- result$extra_cols

  expect_true("extra_col1" %in% extra_cols)
  expect_true("extra_col2" %in% extra_cols)
})

test_that("align_dataframe_to_schema preserves dynamic columns", {
  schema <- get_gld_schema()

  test_df <- copy_to(sc, data.frame(
    hhid = "001",
    pid = "001-01",
    subnatid1 = "Region1",
    subnatid2 = "District1",
    gaul_adm1_code = "12345",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS")
  aligned_df <- result$aligned_df %>% collect()

  expect_true("subnatid1" %in% names(aligned_df))
  expect_true("subnatid2" %in% names(aligned_df))
  expect_true("gaul_adm1_code" %in% names(aligned_df))
  expect_equal(aligned_df$subnatid1[1], "Region1")
})

# COMMAND ----------

# =============================================================================
# Test update_metadata_versions function
# =============================================================================

test_that("update_metadata_versions updates stacked versions using Delta table version", {
  # The function derives new stacked versions from get_delta_table_version(),
  # not from table_version in change_keys. We stub it to return 5.
  with_mocked_delta_version(5, {
    test_metadata <- copy_to(sc, data.frame(
      table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
      classification = "Official Use",
      country = "TST",
      year = 2020,
      survey = "LFS",
      table_version = 1,
      stacked_all_table_version = NA_integer_,
      stacked_ouo_table_version = NA_integer_,
      stringsAsFactors = FALSE
    ), overwrite = TRUE)

    test_changes <- copy_to(sc, data.frame(
      countrycode = "TST",
      year = 2020L,
      survname = "LFS",
      table_version = 1,
      stringsAsFactors = FALSE
    ), overwrite = TRUE)

    updated <- update_metadata_versions(
      test_metadata, test_changes,
      "dummy_all_table", "dummy_ouo_table", sc
    ) %>% collect()

    # Both versions should equal the mocked Delta version (5)
    expect_equal(updated$stacked_all_table_version[1], 5L)
    expect_equal(updated$stacked_ouo_table_version[1], 5L)
  })
})

test_that("update_metadata_versions preserves unchanged records", {
  with_mocked_delta_version(5, {
    test_metadata <- copy_to(sc, data.frame(
      table_name = c(
        "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
        "TEST_2021_LFS_V01_M_V01_A_GLD_ALL"
      ),
      classification = c("Official Use", "Official Use"),
      country = c("TST", "TST"),
      year = c(2020, 2021),
      survey = c("LFS", "LFS"),
      table_version = c(1, 2),
      stacked_all_table_version = c(NA_integer_, 3L),
      stacked_ouo_table_version = c(NA_integer_, 3L),
      stringsAsFactors = FALSE
    ), overwrite = TRUE)

    # Only request update for the 2020 row
    test_changes <- copy_to(sc, data.frame(
      countrycode = "TST",
      year = 2020L,
      survname = "LFS",
      table_version = 1,
      stringsAsFactors = FALSE
    ), overwrite = TRUE)

    updated <- update_metadata_versions(
      test_metadata, test_changes,
      "dummy_all_table", "dummy_ouo_table", sc
    ) %>% collect()

    # 2020 row should be updated to the mocked Delta version
    row_2020 <- updated[updated$country == "TST" & updated$year == 2020, ]
    expect_equal(row_2020$stacked_all_table_version, 5L)

    # 2021 row was not in change_keys and must remain at 3
    row_2021 <- updated[updated$country == "TST" & updated$year == 2021, ]
    expect_equal(row_2021$stacked_all_table_version, 3L)
    expect_equal(row_2021$stacked_ouo_table_version, 3L)
  })
})

# COMMAND ----------

# =============================================================================
# Test validation functions
# =============================================================================

test_that("validate_change_detection returns TRUE when changes are found", {
  test_changes <- copy_to(sc, data.frame(
    countrycode = c("TST", "TST"),
    year = c(2020L, 2021L),
    survname = c("LFS", "LFS"),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- suppressMessages(validate_change_detection(test_changes))

  expect_true(result)
})

test_that("validate_change_detection stops when no changes are found", {
  test_empty <- copy_to(sc, data.frame(
    countrycode = character(0),
    year = integer(0),
    survname = character(0),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  expect_error(
    suppressMessages(validate_change_detection(test_empty)),
    "All tables are up-to-date"
  )
})

test_that("validate_processing_count validates correctly", {
  update_list <- list(
    list(table_name = "TABLE1"),
    list(table_name = "TABLE2"),
    list(table_name = "TABLE3")
  )

  # Processed count matches list length => TRUE
  result <- suppressMessages(validate_processing_count(3, update_list))
  expect_true(result)

  # Processed count does not match => FALSE
  result <- suppressMessages(validate_processing_count(1, update_list))
  expect_false(result)
})

# COMMAND ----------

message("All stacking function tests completed!")
