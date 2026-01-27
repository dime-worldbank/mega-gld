library(dplyr)
library(sparklyr)

source("helpers/filename_parsing.R")

root_dir <- "/Volumes/prd_csc_mega/sgld48/vgld48/Documents"

sc <- spark_connect(method = "databricks")

target_schema  <- "prd_csc_mega.sgld48"
metadata_table <- paste0(target_schema, "._ingestion_metadata")

# --- find harmonized data folders and dta files --- 

country_dirs <- list.dirs(root_dir, recursive = FALSE)

dataset_dirs <- unlist(lapply(country_dirs, list.dirs, recursive = FALSE))

version_dirs <- unlist(lapply(dataset_dirs, function(d) {list.dirs(d, recursive = FALSE)}))

harmonized_paths <- file.path(version_dirs, "Data", "Harmonized")

harmonized_paths <- harmonized_paths[dir.exists(harmonized_paths)]

all_dta_files <- list_dta_files(harmonized_paths)

print("Dta files collected")

parsed <- bind_rows(lapply(all_dta_files, parse_metadata_from_filename))

latest_versions <- filter_latest_versions(parsed)

latest_tables <- latest_versions$dta_path

print("Latest tables filtered")


# --- remove all older-version tables  ---

metadata <- tbl(sc, metadata_table) %>% collect()

old_versions <- setdiff(metadata$dta_path, latest_tables)


if (length(old_versions) > 0) {

  old_meta <- metadata %>% filter(dta_path %in% old_versions)

  for (i in seq_len(nrow(old_meta))) {

    tbl_name <- old_meta$table_name[i]

    if (!is.na(tbl_name) && nzchar(tbl_name)) {

      full_table <- paste0(target_schema, ".", tbl_name)

      message("Deleting old version Delta table: ", full_table)

      DBI::dbExecute(sc, paste0("DROP TABLE IF EXISTS ", full_table))

    }

    DBI::dbExecute(
      sc,
      paste0("
        DELETE FROM ", metadata_table, "
        WHERE dta_path = '", old_meta$dta_path[i], "'
      ")
    )

  }
}

# --- check already ingested and add new files to metadata table ---

metadata <- tbl(sc, metadata_table) %>% collect()

already_ingested <- metadata$dta_path[metadata$ingested == TRUE]
new_files <- setdiff(latest_tables, already_ingested)


if (length(new_files) > 0) {

  dataset_names <- basename(dirname(dirname(dirname(new_files))))
  new_filenames <- unique(dataset_names)
  print(new_filenames)

  meta_details <- latest_versions %>% filter(dta_path %in% new_files)

  new_meta <- tibble(
    filename = dataset_names,
    table_name = NA_character_,
    dta_path = new_files,
    ingested = FALSE,
    published = FALSE,
    harmonization = NA_character_,
    household_level = NA,
    version_label = NA_character_,
    do_path = NA_character_,
    classification = NA_character_
  ) %>%
    left_join(
      meta_details %>%
        select(
          filename,
          country, year, quarter, survey,
          M_version, A_version
        ),
      by = c("filename" = "filename")
    )

  copy_to(sc, new_meta, "tmp_new_meta", overwrite = TRUE)

  cols <- colnames(new_meta)

  DBI::dbExecute(
    sc,
    paste0(
      "
      INSERT INTO ", metadata_table, " (", paste(cols, collapse = ", "), ")
      SELECT ", paste(paste0("t.", cols), collapse = ", "), "
      FROM tmp_new_meta t
      LEFT ANTI JOIN ", metadata_table, " m
      ON t.dta_path = m.dta_path
      "
    )
  )

  DBI::dbExecute(sc, "DROP TABLE IF EXISTS tmp_new_meta")
}
