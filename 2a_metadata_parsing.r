# Databricks notebook source
library(purrr)
library(dplyr)

# COMMAND ----------

# MAGIC %run "./helpers/config"

# COMMAND ----------

# MAGIC %run "./helpers/metadata_parsing"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}
if (!exists("find_do_files")) {
  source("helpers/do_file_parsing.r")
}



if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")

  print("Loading metadata table...")
  metadata <- tbl(sc, METADATA_TABLE) %>% collect()
  print(paste("Loaded", nrow(metadata), "records"))

  updated_metadata <- compute_metadata_updates(metadata)

  if (identical(metadata, updated_metadata)) {
    return(print("No updates needed"))
  }

  print("Updating metadata table...")
  copy_to(sc, updated_metadata, "tmp_new_meta", overwrite = TRUE)

  DBI::dbExecute(
    sc,
    paste0(
      "UPDATE ", METADATA_TABLE, " AS m ",
      "SET
        version_label = (
          SELECT ANY_VALUE(p.version_label)
          FROM tmp_new_meta p
          WHERE p.filename = m.filename
        ),
        do_path = (
          SELECT ANY_VALUE(p.do_path)
          FROM tmp_new_meta p
          WHERE p.filename = m.filename
        ),
        classification = (
          SELECT ANY_VALUE(p.classification)
          FROM tmp_new_meta p
          WHERE p.filename = m.filename
        )
      WHERE m.filename IN (SELECT filename FROM tmp_new_meta)"
    )
  )

  DBI::dbExecute(sc, "DROP TABLE IF EXISTS tmp_new_meta")
  print("Done")
}
