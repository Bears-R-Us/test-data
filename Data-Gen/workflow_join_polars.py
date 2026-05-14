#!/usr/bin/env python3
"""
Standalone workflow join test using Polars.

Reads all tables from parquet (produced by new_datagen_faster.py + create_join_tables.py)
and performs the full chained 4-table left join workflow.
"""
import time
from pathlib import Path

import polars as pl

PATH_PREFIX = Path(__file__).resolve().parent / "test-data" / "500000-1GB"


def scan_tables(path_prefix: str) -> dict[str, pl.LazyFrame]:
    return {
        "table_a": pl.scan_parquet(f"{path_prefix}/temp_forced_table_a/*.parquet"),
        "table_b": pl.scan_parquet(f"{path_prefix}/temp_table_b/*.parquet"),
        "table_c": pl.scan_parquet(f"{path_prefix}/temp_forced_table_c/*.parquet"),
        "table_d": pl.scan_parquet(f"{path_prefix}/temp_forced_table_d/*.parquet"),
        "table_e": pl.scan_parquet(f"{path_prefix}/temp_forced_table_e/*.parquet"),
    }


def print_row_counts(tables: dict[str, pl.LazyFrame]) -> None:
    for name, table in tables.items():
        row_count = table.select(pl.len()).collect().item()
        print(f"  {name}: {row_count} rows")


def build_workflow_join_query(tables: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    table_a = tables["table_a"]
    table_b = tables["table_b"]
    table_c = tables["table_c"].with_columns(pl.col("col_c_11").cast(pl.Utf8))
    table_d = tables["table_d"]
    table_e = tables["table_e"]

    return (
        table_a
        .join(
            table_b,
            left_on=["col_a", "col_b", "col_c", "col_d"],
            right_on=["col_b_8", "col_b_3", "col_b_9", "col_b_1"],
            how="left",
        )
        .join(
            table_c,
            left_on=["col_a", "col_b", "col_e"],
            right_on=["col_c_10", "col_c_9", "col_c_11"],
            how="left",
        )
        .join(
            table_d,
            left_on=["col_a", "col_c"],
            right_on=["col_d_0", "col_d_1"],
            how="left",
        )
        .join(
            table_e,
            left_on=["col_a"],
            right_on=["col_e_0"],
            how="left",
        )
    )


print("Reading tables from parquet...")
tables = scan_tables(PATH_PREFIX)
print_row_counts(tables)

print("\nRunning workflow join: table_a → table_b → table_c → table_d → table_e ...")
workflow_query = build_workflow_join_query(tables)

start = time.time()
workflow_result = workflow_query.collect()
elapsed = time.time() - start

print(f"\nWorkflow join completed in {elapsed:.2f} seconds")
print(f"  Result: {len(workflow_result)} rows, {len(workflow_result.columns)} columns")
