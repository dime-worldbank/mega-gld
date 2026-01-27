%pip install pyreadstat

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, first
from pyspark.sql.utils import AnalysisException
import gc
import pyreadstat


spark = SparkSession.builder.getOrCreate()

target_schema = "prd_csc_mega.sgld48"
metadata_table = f"{target_schema}._ingestion_metadata"

CHUNK_THRESHOLD_MB = 900
CHUNK_SIZE = 5000

# --- load metadata ---

metadata = (
    spark.read.table(metadata_table)
    .select("dta_path","table_name", "country", "year", "survey", "quarter", "M_version", "A_version", "ingested")
    .toPandas()
)

candidates = metadata[metadata["ingested"] == False]

if candidates.empty:
    print("No files to ingest.")
    raise SystemExit




# --- helper functions ---

def update_metadata(dta_path, tbl_name, harm_type, household_level, table_version):
    harm_sql = "NULL" if harm_type is None else f"'{harm_type}'"
    hh_sql = "NULL" if household_level is None else ("TRUE" if household_level else "FALSE")
    tv_sql = "NULL" if table_version is None else str(int(table_version))

    spark.sql(f"""
        UPDATE {metadata_table}
        SET
            ingested = TRUE,
            harmonization = {harm_sql},
            household_level = {hh_sql},
            table_name = '{tbl_name}',
            table_version = {tv_sql}
        WHERE dta_path = '{dta_path}'
    """)

    print("Updated metadata for:", dta_path)

def get_stata_var_labels(dta_path):
    _, meta = pyreadstat.read_dta(dta_path, metadataonly=True)
    return {
        name: label
        for name, label in zip(meta.column_names, meta.column_labels)
        if label is not None and str(label).strip() != ""
    }

def apply_column_comments(full_table_name, var_labels):
    for col_name, label in var_labels.items():
        try:
            if label is None:
                continue

            safe_label = (str(label)
                .replace("\x00", " ")
                .replace("\r", " ")
                .replace("\n", " ")
                .strip()
                .replace("'", "")
            )

            spark.sql(
                f"ALTER TABLE {full_table_name} "
                f"ALTER COLUMN `{col_name}` "
                f"COMMENT '{safe_label}'"
            )

        except Exception as e:
            print(f"Skipping comment for {full_table_name}.{col_name}: {e}")

def compute_stacking(pdf):
    out = pdf.copy()

    # default: stacking = 1
    out["stacking"] = 1

    # add 0 for panel tables
    is_panel = out["table_name"].astype(str).str.contains("panel", case=False, na=False)
    out.loc[is_panel, "stacking"] = 0

    # annual rows only (quarter null/empty) can conflict on country-year
    is_annual = out["quarter"].isna() | (out["quarter"].astype(str).str.strip() == "")
    annual = out[is_annual]

    # duplicates among annual (country, year)
    conflict_mask = annual.duplicated(subset=["country", "year"], keep=False)

    if conflict_mask.any():
        conflicted = annual[conflict_mask].copy()

        # priority: GLD over GLD-Light
        harm_prio = (
            conflicted["harmonization"]
            .map({"GLD": 2, "GLD-Light": 1})
            .fillna(0)
            .astype(int)
        )

        conflicted["harm_prio"] = harm_prio

        # best first
        conflicted = conflicted.sort_values(
            ["country", "year", "harm_prio", "M_version", "A_version"],
            ascending=[True, True, False, False, False],
        )

        # pick winner per (country, year)
        winners = conflicted.drop_duplicates(
            subset=["country", "year"], keep="first"
        ).index

        # set all conflicted annual rows to 0, then winners to 1
        out.loc[conflicted.index, "stacking"] = 0
        out.loc[winners, "stacking"] = 1

        # panel always stays 0
        out.loc[is_panel, "stacking"] = 0

    return out

def get_table_version(full_table_name):
    hist = spark.sql(f"DESCRIBE HISTORY {full_table_name}")
    return int(hist.select("version").orderBy(col("version").desc()).first()["version"])

def table_exists(full_table_name):
    return spark.catalog.tableExists(full_table_name)


# --- define ingestion order ---

key_cols = ["country", "year", "survey", "quarter"]

to_ingest = candidates.sort_values(
    key_cols + ["M_version", "A_version"],
    ascending=[True, True, True, True, True, True]
)

print("Files to ingest:", len(to_ingest))


# --- ingest ----

for _, row in to_ingest.iterrows():

    dta_path = row["dta_path"]
    tbl_name = str(row["table_name"]).strip()

    if not tbl_name or tbl_name.lower() == "nan":
        print(f"SKIPPING: missing table_name for dta_path={dta_path}")
        continue
    
    full_write_table = f"{target_schema}.{tbl_name}"    
    print("--- Processing:", dta_path)

    try:
        exists = table_exists(full_write_table)
        if exists:
            print("Table exists → overwriting")
        else:
            print("Table does not exist → creating")
        
        size_mb = os.path.getsize(dta_path) / (1024 * 1024)
        
        if size_mb <= CHUNK_THRESHOLD_MB:

            pdf = pd.read_stata(dta_path, convert_categoricals=False)

            cols = pdf.columns

            harm_type = (pdf["harmonization"].iloc[0] if "harmonization" in cols else None)
            household_level = ("hhid" in cols and pdf["hhid"].notna().any())

            sdf = spark.createDataFrame(pdf)

            print("Writing:", full_write_table)
            sdf.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(full_write_table)

            var_labels = get_stata_var_labels(dta_path)
            #print("labels:", list(var_labels.items())[:10])
            apply_column_comments(full_write_table, var_labels)
            table_version = get_table_version(full_write_table)
            update_metadata(dta_path, tbl_name, harm_type, household_level, table_version)

            del pdf, sdf
            gc.collect()
            continue

        print("Chunking:", full_write_table)

        reader = pd.read_stata(dta_path, chunksize=CHUNK_SIZE, iterator=True, convert_categoricals=False)

        first_chunk = next(reader)
        print("First chunk size:",first_chunk.memory_usage(deep=True).sum() / 1_048_576,"MB")

        cols = first_chunk.columns

        harm_type = (first_chunk["harmonization"].iloc[0] if "harmonization" in cols else None)
        household_level = ("hhid" in cols and first_chunk["hhid"].notna().any())

        var_labels = get_stata_var_labels(dta_path)

        #print("labels:", list(var_labels.items())[:10])

        spark_df = spark.createDataFrame(first_chunk)
        spark_schema = spark_df.schema

        print("Writing first chunk →", full_write_table)
        spark_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(full_write_table)

        apply_column_comments(full_write_table, var_labels)

        del first_chunk
        del spark_df
        gc.collect()

        for i, chunk in enumerate(reader, start=2):
            print(f"Chunk {i} size:", chunk.memory_usage(deep=True).sum() / 1_048_576, "MB")

            spark_df = spark.createDataFrame(chunk, schema=spark_schema)
            spark_df.write.mode("append").format("delta").saveAsTable(full_write_table)

            del chunk
            del spark_df
            gc.collect()

        # --- update metadata ---
        table_version = get_table_version(full_write_table)
        update_metadata(dta_path, tbl_name, harm_type, household_level, table_version)
    
    except Exception as e:
        print(f"ERROR ingesting {dta_path}: {e}")

    for obj in ["reader", "pdf", "first_chunk", "chunk", "reader", "spark_df", "spark_schema"]:
        if obj in locals():
            del locals()[obj]
    gc.collect()


# --- update stacking for all ingested tables ---

metadata = spark.read.table(metadata_table).toPandas()
metadata = metadata[metadata["ingested"] == True].copy()

metadata = compute_stacking(metadata)

updates = metadata[["dta_path", "stacking"]].drop_duplicates()

spark.createDataFrame(updates).createOrReplaceTempView("stacking_updates")

spark.sql(f"""
MERGE INTO {metadata_table} t
USING stacking_updates s
ON t.dta_path = s.dta_path
WHEN MATCHED THEN UPDATE SET t.stacking = s.stacking
""")

print("Done: stacking updated.")



