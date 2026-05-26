#!/usr/bin/env python3
import argparse
import os
import shutil
import time
from pathlib import Path

import polars as pl
from cudf_polars.engine.options import StreamingOptions
from cudf_polars.engine.ray import RayEngine
from cudf_polars.utils.config import MemoryResourceConfig

PATH_PREFIX = "/scratch/prestouser/test-data/500000-1TB"
OUTPUT = Path(PATH_PREFIX) / "workflow_join_polars_ray_output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run workflow join with cudf-polars RayEngine."
    )
    parser.add_argument("--path-prefix", default=os.environ.get("DATASET_PATH", PATH_PREFIX))
    parser.add_argument("--output", default=os.environ.get("OUTPUT_PATH", str(OUTPUT)))
    parser.add_argument("--ray-address", default=os.environ.get("RAY_ADDRESS"))
    parser.add_argument("--num-py-executors", type=int, default=8)
    parser.add_argument("--target-partition-size", type=int, default=3_221_225_472)
    parser.add_argument("--spill-device-limit", default="70%")
    parser.add_argument("--pinned-initial-pool-size", type=int, default=51_539_607_552)
    parser.add_argument("--rmm-release-threshold", type=int, default=160_000_000_000)
    return parser.parse_args()


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


def main() -> None:
    args = parse_args()
    path_prefix = str(args.path_prefix)
    output = Path(args.output)

    print("Reading tables from parquet...")
    print(f"  path_prefix: {path_prefix}")
    tables = scan_tables(path_prefix)
    print_row_counts(tables)

    print("\nRunning workflow join: table_a -> table_b -> table_c -> table_d -> table_e ...")
    workflow_query = build_workflow_join_query(tables)

    memory_resource_config = MemoryResourceConfig(
        qualname="rmm.mr.CudaAsyncMemoryResource",
        options={"release_threshold": args.rmm_release_threshold},
    )

    streaming_options = StreamingOptions(
        spill_device_limit=args.spill_device_limit,
        pinned_memory=True,
        pinned_initial_pool_size=args.pinned_initial_pool_size,
        statistics=True,
        num_py_executors=args.num_py_executors,
        fallback_mode="silent",
        target_partition_size=args.target_partition_size,
        memory_resource_config=memory_resource_config,
    )

    ray_init_options = {}
    if args.ray_address:
        ray_init_options["address"] = args.ray_address
        print(f"Connecting to Ray cluster at {args.ray_address}")

    with RayEngine.from_options(
        streaming_options, ray_init_options=ray_init_options
    ) as engine:
        print(f"RayEngine ranks: {engine.nranks}")
        prepare_output_path(output)
        start = time.time()
        workflow_query.sink_parquet(output, engine=engine, mkdir=True)
        elapsed = time.time() - start
        statistics = engine.global_statistics()

    print(f"\nWorkflow join completed in {elapsed:.2f} seconds")
    print(f"  Output: {output}")
    row_count = pl.scan_parquet(f"{output}/*.parquet").select(pl.len()).collect().item()
    print(f"  Result rows: {row_count}")
    print("\nRapidsMPF statistics:")
    print(statistics.report())


if __name__ == "__main__":
    main()
