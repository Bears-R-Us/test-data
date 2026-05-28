#!/usr/bin/env python3
import argparse
import os
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

PATH_PREFIX = "/scratch/prestouser/test-data/500000-1TB"
OUTPUT = f"{PATH_PREFIX}/workflow_join_spark_rapids_output"
SPARK_RAPIDS_JAR = "/opt/spark/sparkRapidsPlugin/rapids-4-spark.jar"


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the 1TB workflow join with Spark or Spark RAPIDS."
    )
    parser.add_argument("--path-prefix", default=os.environ.get("DATASET_PATH", PATH_PREFIX))
    parser.add_argument("--output", default=os.environ.get("OUTPUT_PATH", OUTPUT))
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER_URL"))
    parser.add_argument("--plugin", choices=("rapids", "cpu"), default=os.environ.get("SPARK_PLUGIN", "rapids"))
    parser.add_argument("--app-name", default=os.environ.get("SPARK_APP_NAME", "WorkflowJoinSparkRapids1TB"))
    parser.add_argument("--action", choices=("parquet", "count", "noop"), default=os.environ.get("WORKFLOW_ACTION", "parquet"))
    parser.add_argument("--mode", default=os.environ.get("SPARK_WRITE_MODE", "overwrite"))
    parser.add_argument("--output-partitions", type=int, default=int(os.environ.get("OUTPUT_PARTITIONS", "0")))
    parser.add_argument("--count-inputs", action="store_true", default=env_bool("COUNT_INPUTS", False))
    parser.add_argument("--count-result", action="store_true", default=env_bool("COUNT_RESULT", False))
    parser.add_argument("--explain", action="store_true", default=env_bool("SPARK_EXPLAIN", False))
    parser.add_argument("--event-log-dir", default=os.environ.get("SPARK_EVENT_LOG_DIR", "file:///scratch/prestouser/spark-events"))
    parser.add_argument("--driver-host", default=os.environ.get("SPARK_DRIVER_HOST"))
    parser.add_argument("--executor-instances", type=int, default=int(os.environ.get("SPARK_EXECUTOR_INSTANCES", "0")))
    parser.add_argument("--executor-cores", type=int, default=int(os.environ.get("SPARK_EXECUTOR_CORES", "8")))
    parser.add_argument("--executor-memory", default=os.environ.get("SPARK_EXECUTOR_MEMORY", "96g"))
    parser.add_argument("--executor-memory-overhead", default=os.environ.get("SPARK_EXECUTOR_MEMORY_OVERHEAD", "64g"))
    parser.add_argument("--driver-memory", default=os.environ.get("SPARK_DRIVER_MEMORY", "32g"))
    parser.add_argument("--shuffle-partitions", type=int, default=int(os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "2400")))
    parser.add_argument("--files-max-partition-bytes", default=os.environ.get("SPARK_SQL_FILES_MAX_PARTITION_BYTES", "160m"))
    parser.add_argument("--advisory-partition-size", default=os.environ.get("SPARK_SQL_ADVISORY_PARTITION_SIZE", "160mb"))
    parser.add_argument("--min-partition-size", default=os.environ.get("SPARK_SQL_MIN_PARTITION_SIZE", "32mb"))
    parser.add_argument("--rapids-concurrent-gpu-tasks", type=int, default=int(os.environ.get("SPARK_RAPIDS_CONCURRENT_GPU_TASKS", "1")))
    parser.add_argument("--task-gpu-amount", default=os.environ.get("SPARK_TASK_GPU_AMOUNT", "0.125"))
    parser.add_argument("--rapids-pinned-pool-size", default=os.environ.get("SPARK_RAPIDS_PINNED_POOL_SIZE", "4g"))
    parser.add_argument("--rapids-host-spill-size", default=os.environ.get("SPARK_RAPIDS_HOST_SPILL_SIZE", "128G"))
    parser.add_argument("--rapids-batch-size-bytes", default=os.environ.get("SPARK_RAPIDS_SQL_BATCH_SIZE_BYTES", "536870912b"))
    parser.add_argument("--rapids-shuffle-reader-threads", default=os.environ.get("SPARK_RAPIDS_SHUFFLE_READER_THREADS", "8"))
    parser.add_argument("--rapids-shuffle-writer-threads", default=os.environ.get("SPARK_RAPIDS_SHUFFLE_WRITER_THREADS", "8"))
    parser.add_argument("--rapids-shuffle-manager", default=os.environ.get("SPARK_RAPIDS_SHUFFLE_MANAGER", "com.nvidia.spark.rapids.spark358.RapidsShuffleManager"))
    parser.add_argument("--rapids-explain", default=os.environ.get("SPARK_RAPIDS_SQL_EXPLAIN", "NONE"))
    return parser.parse_args()


def config_if(builder: SparkSession.Builder, key: str, value: object | None) -> SparkSession.Builder:
    if value not in (None, "", 0):
        builder = builder.config(key, value)
    return builder


def create_session(args: argparse.Namespace) -> SparkSession:
    builder = SparkSession.builder.appName(args.app_name)
    if args.master:
        builder = builder.master(args.master)

    common_configs = {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": args.event_log_dir,
        "spark.ui.prometheus.enabled": "true",
        "spark.metrics.namespace": args.app_name,
        "spark.submit.deployMode": "client",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.executor.extraJavaOptions": "-Djava.net.preferIPv4Stack=true",
        "spark.driver.extraJavaOptions": "-Djava.net.preferIPv4Stack=true",
        "spark.sql.debug.maxToStringFields": "1000",
        "spark.executor.heartbeatInterval": "60s",
        "spark.network.timeout": "300s",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": args.advisory_partition_size,
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": args.min_partition_size,
        "spark.sql.files.maxPartitionBytes": args.files_max_partition_bytes,
        "spark.sql.shuffle.partitions": str(args.shuffle_partitions),
        "spark.task.cpus": "1",
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.sql.legacy.charVarcharAsString": "true",
        "spark.locality.wait": "0",
        "spark.executor.cores": str(args.executor_cores),
        "spark.executor.memory": args.executor_memory,
        "spark.executor.memoryOverhead": args.executor_memory_overhead,
        "spark.driver.memory": args.driver_memory,
    }
    if args.driver_host:
        common_configs["spark.driver.host"] = args.driver_host
    if args.executor_instances:
        common_configs["spark.executor.instances"] = str(args.executor_instances)

    for key, value in common_configs.items():
        builder = builder.config(key, value)

    if args.plugin == "rapids":
        rapids_configs = {
            "spark.driver.extraClassPath": SPARK_RAPIDS_JAR,
            "spark.executor.extraClassPath": SPARK_RAPIDS_JAR,
            "spark.plugins": "com.nvidia.spark.SQLPlugin",
            "spark.rapids.sql.enabled": "true",
            "spark.rapids.sql.explain": args.rapids_explain,
            "spark.rapids.sql.exec.InMemoryTableScanExec": "true",
            "spark.rapids.allowMultipleJars": "ALWAYS",
            "spark.rapids.sql.allowMultipleJars": "ALWAYS",
            "spark.rapids.filecache.enabled": "false",
            "spark.rapids.memory.gpu.debug": "NONE",
            "spark.rapids.memory.host.spillStorageSize": args.rapids_host_spill_size,
            "spark.rapids.memory.pinnedPool.size": args.rapids_pinned_pool_size,
            "spark.rapids.shuffle.mode": "MULTITHREADED",
            "spark.rapids.shuffle.multiThreaded.reader.threads": args.rapids_shuffle_reader_threads,
            "spark.rapids.shuffle.multiThreaded.writer.threads": args.rapids_shuffle_writer_threads,
            "spark.rapids.sql.batchSizeBytes": args.rapids_batch_size_bytes,
            "spark.rapids.sql.concurrentGpuTasks": str(args.rapids_concurrent_gpu_tasks),
            "spark.shuffle.manager": args.rapids_shuffle_manager,
            "spark.shuffle.compress": "true",
            "spark.executor.resource.gpu.amount": "1",
            "spark.task.resource.gpu.amount": str(args.task_gpu_amount),
            "spark.executor.resource.gpu.vendor": "nvidia.com",
        }
        for key, value in rapids_configs.items():
            builder = builder.config(key, value)
    else:
        builder = builder.config("spark.rapids.sql.enabled", "false")

    return builder.getOrCreate()


def read_tables(spark: SparkSession, path_prefix: str) -> dict[str, DataFrame]:
    return {
        "table_a": spark.read.parquet(f"{path_prefix}/temp_forced_table_a"),
        "table_b": spark.read.parquet(f"{path_prefix}/temp_table_b"),
        "table_c": spark.read.parquet(f"{path_prefix}/temp_forced_table_c").withColumn("col_c_11", col("col_c_11").cast("string")),
        "table_d": spark.read.parquet(f"{path_prefix}/temp_forced_table_d"),
        "table_e": spark.read.parquet(f"{path_prefix}/temp_forced_table_e"),
    }


def print_row_counts(tables: dict[str, DataFrame]) -> None:
    for name, table in tables.items():
        print(f"  {name}: {table.count()} rows", flush=True)


def build_workflow_join_query(tables: dict[str, DataFrame]) -> DataFrame:
    table_a = tables["table_a"].alias("a")
    table_b = tables["table_b"].alias("b")
    table_c = tables["table_c"].alias("c")
    table_d = tables["table_d"].alias("d")
    table_e = tables["table_e"].alias("e")

    result = (
        table_a
        .join(
            table_b,
            [
                col("a.col_a") == col("b.col_b_8"),
                col("a.col_b") == col("b.col_b_3"),
                col("a.col_c") == col("b.col_b_9"),
                col("a.col_d") == col("b.col_b_1"),
            ],
            how="left",
        )
        .drop("col_b_8", "col_b_3", "col_b_9", "col_b_1")
    )
    result = (
        result
        .join(
            table_c,
            [
                result["col_a"] == col("c.col_c_10"),
                result["col_b"] == col("c.col_c_9"),
                result["col_e"] == col("c.col_c_11"),
            ],
            how="left",
        )
        .drop("col_c_10", "col_c_9", "col_c_11")
    )
    result = (
        result
        .join(
            table_d,
            [
                result["col_a"] == col("d.col_d_0"),
                result["col_c"] == col("d.col_d_1"),
            ],
            how="left",
        )
        .drop("col_d_0", "col_d_1")
    )
    return result.join(table_e, result["col_a"] == col("e.col_e_0"), how="left").drop("col_e_0")


def print_effective_config(spark: SparkSession, args: argparse.Namespace) -> None:
    keys = [
        "spark.master",
        "spark.driver.host",
        "spark.executor.instances",
        "spark.executor.cores",
        "spark.executor.memory",
        "spark.executor.resource.gpu.amount",
        "spark.task.resource.gpu.amount",
        "spark.rapids.sql.enabled",
        "spark.rapids.sql.concurrentGpuTasks",
        "spark.rapids.memory.pinnedPool.size",
        "spark.rapids.sql.batchSizeBytes",
        "spark.shuffle.manager",
        "spark.sql.shuffle.partitions",
        "spark.sql.files.maxPartitionBytes",
    ]
    print("Effective Spark config:", flush=True)
    for key in keys:
        print(f"  {key}: {spark.conf.get(key, '<unset>')}", flush=True)
    print(f"  plugin: {args.plugin}", flush=True)


def main() -> None:
    args = parse_args()
    path_prefix = str(Path(args.path_prefix))
    output = str(Path(args.output))

    print("Starting Spark workflow join", flush=True)
    print(f"  path_prefix: {path_prefix}", flush=True)
    print(f"  output:      {output}", flush=True)
    print(f"  action:      {args.action}", flush=True)

    spark = create_session(args)
    print_effective_config(spark, args)

    try:
        print("Reading tables from parquet...", flush=True)
        tables = read_tables(spark, path_prefix)
        if args.count_inputs:
            print("Input row counts:", flush=True)
            print_row_counts(tables)
        else:
            print("Skipping input row counts.", flush=True)

        print("Building workflow join: table_a -> table_b -> table_c -> table_d -> table_e ...", flush=True)
        workflow_result = build_workflow_join_query(tables)
        if args.output_partitions > 0:
            print(f"Repartitioning result to {args.output_partitions} partitions before action.", flush=True)
            workflow_result = workflow_result.repartition(args.output_partitions)
        if args.explain:
            workflow_result.explain(mode="formatted")

        start = time.time()
        result_rows = None
        if args.action == "parquet":
            workflow_result.write.mode(args.mode).parquet(output)
            if args.count_result:
                result_rows = spark.read.parquet(output).count()
        elif args.action == "count":
            result_rows = workflow_result.count()
        else:
            workflow_result.write.format("noop").mode("overwrite").save()
        elapsed = time.time() - start

        print(f"\nWorkflow join completed in {elapsed:.2f} seconds", flush=True)
        if args.action == "parquet":
            print(f"  Output: {output}", flush=True)
        if result_rows is not None:
            print(f"  Result rows: {result_rows}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
