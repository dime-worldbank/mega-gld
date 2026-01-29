# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(purrr)
  library(readr)
  library(withr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/config"

# COMMAND ----------

# MAGIC %run "../../helpers/txt_parsing"

# COMMAND ----------

# MAGIC %run "../../helpers/do_file_parsing"

# COMMAND ----------

#! This is the Databricks fixture_path. needs to be updated. 
fixture_path <- normalizePath(
    file.path(repo_root, "tests", "fixtures", "sample_metadata_2a_input.csv"),
    mustWork = TRUE
  )

# COMMAND ----------

if (!exists("find_txt_files")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "config.r"))
  source(file.path(repo_root, "helpers", "txt_parsing.r"))
  source(file.path(repo_root, "helpers", "do_file_parsing.r"))

  fixture_path <- normalizePath(
    file.path(repo_root, "tests", "fixtures", "sample_metadata_2a_input.csv"),
    mustWork = TRUE
  )
}

# COMMAND ----------

  
test_that("metadata parsing pipeline updates do_path and version_label for unpublished rows", {
  metadata <- readr::read_csv(fixture_path, show_col_types = FALSE)

  tmp <- tempdir()
  base_dir <- file.path(tmp, "meta_parse")

  metadata <- metadata %>%
    mutate(
      dta_path = gsub("^BASE", base_dir, dta_path),
      do_path = ifelse(is.na(do_path), NA_character_, gsub("^BASE", base_dir, do_path))
    )
    

  unpublished <- metadata %>%
    filter(published == FALSE, is.na(do_path))

  for (i in seq_len(nrow(unpublished))) {
    row <- unpublished[i, ]
    harmonized_dir <- dirname(row$dta_path)
    version_dir <- dirname(dirname(harmonized_dir))
    programs_dir <- file.path(version_dir, "Programs")

    dir.create(programs_dir, recursive = TRUE, showWarnings = FALSE)

    if (row$filename == "AAA_2020_SURV_V01_M_V01_A_GLD") {
      do_file <- file.path(programs_dir, "AAA_2020_SURV_V01_M_V01_A_GLD_ALL.do")
      writeLines(c(
        "<_Version Control_>",
        "* 2021-01-01 - Initial version",
        "* 2022-02-02 - Added feature",
        "</_Version Control_>"
      ), do_file)
    }

    if (row$filename == "BBB_2019_SURV_V01_M_V01_A_GLD") {
      do_file <- file.path(programs_dir, "BBB_2019_SURV_V01_M_V01_A_GLD_ALL.do")
      writeLines(c(
        "<_Version Control_>",
        "* 2020-01-01 - Initial",
        "* 2021-01-01 - Description of changes",
        "</_Version Control_>"
      ), do_file)
    }
  }

  metadata <- compute_metadata_updates(metadata)

  row_aaa <- metadata %>% filter(filename == "AAA_2020_SURV_V01_M_V01_A_GLD")
  expect_true(grepl("AAA_2020_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_aaa$do_path))
  expect_equal(row_aaa$version_label[[1]], "Added feature")

  row_bbb <- metadata %>% filter(filename == "BBB_2019_SURV_V01_M_V01_A_GLD")
  expect_true(grepl("BBB_2019_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_bbb$do_path))
  expect_equal(row_bbb$version_label[[1]], "Initial")

  row_ccc <- metadata %>% filter(filename == "CCC_2018_SURV_V01_M_V01_A_GLD")
  expect_equal(row_ccc$version_label[[1]], "Existing label")
  expect_true(grepl("CCC_2018_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_ccc$do_path)
  )
})


test_that("metadata parsing pipeline updates classification for unpublished rows missing classification", {

  metadata <- readr::read_csv(fixture_path, show_col_types = FALSE)

  tmp <- tempdir()
  base_dir <- file.path(tmp, "meta_parse_class")

  metadata <- metadata %>%
    mutate(
      dta_path = gsub("^BASE", base_dir, dta_path),
      do_path  = ifelse(is.na(do_path), NA_character_, gsub("^BASE", base_dir, do_path))
    )

  unpublished_class <- metadata %>%
    filter(
      published == FALSE,
      is.na(classification) | trimws(classification) == ""
    )

  if (nrow(unpublished_class) == 0) {
    updated <- compute_metadata_updates(metadata)
    expect_true(identical(metadata, updated))
    return(invisible(NULL))
  }

  # Create Doc/Technical/*ReadMe.txt per row so find_txt_files() finds exactly one
  for (i in seq_len(nrow(unpublished_class))) {
    row <- unpublished_class[i, ]

    harmonized_dir <- dirname(row$dta_path)
    version_dir    <- dirname(dirname(harmonized_dir))   # .../v01
    tech_dir       <- file.path(version_dir, "Doc", "Technical")
    dir.create(tech_dir, recursive = TRUE, showWarnings = FALSE)

    txt_file <- file.path(tech_dir, paste0(row$filename, "_ReadMe.txt"))

    # Deterministic content
    if (row$filename == "AAA_2020_SURV_V01_M_V01_A_GLD") {
      writeLines(c("Classification: OFFICIAL_USE", "Classification_code: OUO"), txt_file)
    } else if (row$filename == "BBB_2019_SURV_V01_M_V01_A_GLD") {
      writeLines(c("Classification: CONFIDENTIAL", "Classification_code: CONF"), txt_file)
    } else {
      # fallback heuristic path
      writeLines(c("This file is for internal use only.", "No specific terms."), txt_file)
    }
  }

  updated <- compute_metadata_updates(metadata)

  # All previously-missing classification rows should now be non-empty
  updated_missing <- updated %>%
    filter(
      published == FALSE,
      filename %in% unpublished_class$filename
    )

  expect_true(all(!is.na(updated_missing$classification)))
  expect_true(all(trimws(updated_missing$classification) != ""))

  # Specific expectations if those rows exist in the fixture
  if ("AAA_2020_SURV_V01_M_V01_A_GLD" %in% unpublished_class$filename) {
    aaa <- updated %>% filter(filename == "AAA_2020_SURV_V01_M_V01_A_GLD")
    expect_equal(aaa$classification[[1]], "Official Use")
  }

  if ("BBB_2019_SURV_V01_M_V01_A_GLD" %in% unpublished_class$filename) {
    bbb <- updated %>% filter(filename == "BBB_2019_SURV_V01_M_V01_A_GLD")
    expect_equal(bbb$classification[[1]], "Confidential")
  }
})
