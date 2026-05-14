#!/usr/bin/env python3
"""
Standalone workflow join test.

Reads all tables from parquet (produced by new_datagen_faster.py + create_join_tables.py)
and performs the full chained 4-table left join workflow.
"""
import time
from pathlib import Path

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("WorkflowJoin")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "32g")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.network.timeout", "300s")
    .getOrCreate()
)

PATH_PREFIX = Path(__file__).resolve().parent / "test-data" / "500000-1GB"

# ─── Read all tables from parquet ────────────────────────────────────────────
print("Reading tables from parquet...")
table_a = spark.read.parquet(f"{PATH_PREFIX}/temp_forced_table_a")
table_b = spark.read.parquet(f"{PATH_PREFIX}/temp_table_b")
table_c = spark.read.parquet(f"{PATH_PREFIX}/temp_forced_table_c")
table_d = spark.read.parquet(f"{PATH_PREFIX}/temp_forced_table_d")
table_e = spark.read.parquet(f"{PATH_PREFIX}/temp_forced_table_e")

print(f"  table_a: {table_a.count()} rows")
print(f"  table_b: {table_b.count()} rows")
print(f"  table_c: {table_c.count()} rows")
print(f"  table_d: {table_d.count()} rows")
print(f"  table_e: {table_e.count()} rows")

# ─── Workflow Join ───────────────────────────────────────────────────────────
print("\nRunning workflow join: table_a → table_b → table_c → table_d → table_e ...")
start = time.time()

workflow_result = (
    table_a
    .join(
        table_b,
        [
            table_a["col_a"] == table_b["col_b_8"],
            table_a["col_b"] == table_b["col_b_3"],
            table_a["col_c"] == table_b["col_b_9"],
            table_a["col_d"] == table_b["col_b_1"],
        ],
        how="left",
    )
    .join(
        table_c,
        [
            table_a["col_a"] == table_c["col_c_10"],
            table_a["col_b"] == table_c["col_c_9"],
            table_a["col_e"] == table_c["col_c_11"].cast("string"),
        ],
        how="left",
    )
    .join(
        table_d,
        [
            table_a["col_a"] == table_d["col_d_0"],
            table_a["col_c"] == table_d["col_d_1"],
        ],
        how="left",
    )
    .join(
        table_e,
        table_a["col_a"] == table_e["col_e_0"],
        how="left",
    )
)

workflow_result.write.format("noop").mode("overwrite").save()

elapsed = time.time() - start
print(f"\nWorkflow join completed in {elapsed:.2f} seconds")

spark.stop()
