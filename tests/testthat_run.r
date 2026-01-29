# Databricks notebook source
# MAGIC %run "../helpers/config"

# COMMAND ----------

if (!exists("is_databricks")) {
  library(testthat)
  test_dir(file.path("tests", "testthat"))
  stop()
}

# COMMAND ----------

# MAGIC %run ./testthat/test_0_integration

# COMMAND ----------

# MAGIC %run ./testthat/test_2a_integration_alt

# COMMAND ----------

# MAGIC %run ./testthat/test_do_file_parsing

# COMMAND ----------

# MAGIC %run ./testthat/test_filename_parsing

# COMMAND ----------

# MAGIC %run ./testthat/test_github_links_parsing

# COMMAND ----------

# MAGIC %run ./testthat/test_json_builder

# COMMAND ----------

# MAGIC %run ./testthat/test_json_pipeline
