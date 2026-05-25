#!/usr/bin/env python3
"""
Standalone workflow join test using Polars.

Reads all tables from parquet (produced by new_datagen_faster.py + create_join_tables.py)
and performs the full chained 4-table left join workflow.
"""
import time
import shutil
from pathlib import Path

import polars as pl
from cudf_polars.engine.options import StreamingOptions
from cudf_polars.engine.ray import RayEngine
from cudf_polars.utils.config import MemoryResourceConfig

PATH_PREFIX = Path(__file__).resolve().parent / "test-data" / "500000-1GB"
PATH_PREFIX = "/scratch/prestouser/test-data/500000-50GB"
OUTPUT = Path(PATH_PREFIX) / "workflow_join_polars_output"


def prepare_output_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()

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

RMM_MEMORY_RESOURCE_CONFIG = MemoryResourceConfig(
    qualname="rmm.mr.CudaAsyncMemoryResource",
    options={"release_threshold": 160_000_000_000},
)

STREAMING_OPTIONS = StreamingOptions(
    # RapidsMPF options
    spill_device_limit="70%",
    pinned_memory=True,
    pinned_initial_pool_size=51_539_607_552,
    statistics=True,
    # Executor options
    num_py_executors=8,
    fallback_mode="silent",
    target_partition_size=3_221_225_472,
    # Engine options
    memory_resource_config=RMM_MEMORY_RESOURCE_CONFIG,
)

engine = RayEngine.from_options(STREAMING_OPTIONS)
try:
    prepare_output_path(OUTPUT)
    start = time.time()
    workflow_query.sink_parquet(OUTPUT, engine=engine, mkdir=True)
    elapsed = time.time() - start
    statistics = engine.global_statistics()
finally:
    engine.shutdown()

print(f"\nWorkflow join completed in {elapsed:.2f} seconds")
print(f"  Output: {OUTPUT}")
print(f"  Result rows: {pl.scan_parquet(f'{OUTPUT}/*.parquet').select(pl.len()).collect().item()}")
print("\nRapidsMPF statistics:")
print(statistics.report())
