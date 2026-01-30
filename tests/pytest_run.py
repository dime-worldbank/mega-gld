# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import pytest

# COMMAND ----------

pytest.main([
    "-q",
    "--disable-warnings",
    "pytests/test_ingestion_pipeline.py",
])
