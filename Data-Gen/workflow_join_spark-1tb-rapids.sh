#!/bin/bash
# Multi-node Spark standalone launcher for workflow_join_spark-1tb-rapids.py.
set -euo pipefail
set -x

source /etc/profile.d/spark-rapids.sh

export PYTHONUNBUFFERED=1
export NVIDIA_VISIBLE_DEVICES=${NVIDIA_VISIBLE_DEVICES:-all}
export NVIDIA_DRIVER_CAPABILITIES=${NVIDIA_DRIVER_CAPABILITIES:-compute,utility}

PROJECT_DIR=${REAL_HOME:-$HOME}/bzaitlen/test-data/Data-Gen
SCHEDULER_DIR=${PROJECT_DIR}/scheduler
RESULTS_DIR=${PROJECT_DIR}/results
SPARK_MASTER_URL_FILE=${SCHEDULER_DIR}/spark-master-url-job${SLURM_JOB_ID}.txt
DONE_FLAG=${SCHEDULER_DIR}/spark-done-job${SLURM_JOB_ID}.flag
FAILED_FLAG=${SCHEDULER_DIR}/spark-failed-job${SLURM_JOB_ID}.flag
mkdir -p "$SCHEDULER_DIR" "$RESULTS_DIR"

CURRENT_NODE=$(hostname -s)
NODE_GPUS=${SLURM_GPUS_ON_NODE:-4}
EXPECTED_WORKERS=${SLURM_NNODES:-1}
EXPECTED_GPUS=$((NODE_GPUS * EXPECTED_WORKERS))

SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT:-18080}
SPARK_WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT:-18081}
SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-${SLURM_CPUS_PER_TASK:-144}}
SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-1400g}

SPARK_BASE_DIR=${SPARK_BASE_DIR:-/scratch/prestouser/spark-tmp}
SPARK_NODE_DIR=${SPARK_BASE_DIR}/job-${SLURM_JOB_ID}/${CURRENT_NODE}
export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS:-${SPARK_NODE_DIR}/local}
export SPARK_WORKER_DIR=${SPARK_WORKER_DIR:-${SPARK_NODE_DIR}/worker}
export SPARK_LOG_DIR=${SPARK_LOG_DIR:-${SPARK_NODE_DIR}/logs}
export SPARK_PID_DIR=${SPARK_PID_DIR:-${SPARK_NODE_DIR}/pids}
export SPARK_RUN_DIR=${SPARK_RUN_DIR:-${SPARK_NODE_DIR}/run}
mkdir -p "$SPARK_LOCAL_DIRS" "$SPARK_WORKER_DIR" "$SPARK_LOG_DIR" "$SPARK_PID_DIR" "$SPARK_RUN_DIR"

EVENT_LOG_ROOT=${SPARK_EVENT_LOG_ROOT:-/scratch/prestouser/spark-events}
EVENT_LOG_PATH=${EVENT_LOG_ROOT}/job-${SLURM_JOB_ID}
mkdir -p "$EVENT_LOG_PATH"
export SPARK_EVENT_LOG_DIR=${SPARK_EVENT_LOG_DIR:-file://${EVENT_LOG_PATH}}

export SPARK_DAEMON_MEMORY=${SPARK_DAEMON_MEMORY:-4g}
export SPARK_DAEMON_JAVA_OPTS="${SPARK_DAEMON_JAVA_OPTS:-} -Djava.net.preferIPv4Stack=true"
export SPARK_MASTER_OPTS="${SPARK_MASTER_OPTS:-} -Dspark.deploy.defaultCores=$((EXPECTED_WORKERS * SPARK_WORKER_CORES)) -Dspark.master.rest.enabled=false"
export SPARK_WORKER_OPTS="${SPARK_WORKER_OPTS:-} -Djava.net.preferIPv4Stack=true -Dspark.worker.resource.gpu.amount=${NODE_GPUS} -Dspark.worker.resource.gpu.discoveryScript=${SPARK_HOME}/getGpusResources.sh"
export SPARK_PUBLIC_DNS=${SPARK_PUBLIC_DNS:-$CURRENT_NODE}

echo "Node: $CURRENT_NODE  Head: $HEAD_NODE  NodeID: $SLURM_NODEID  Procid: $SLURM_PROCID"
echo "[$CURRENT_NODE] NODE_GPUS=$NODE_GPUS EXPECTED_WORKERS=$EXPECTED_WORKERS EXPECTED_GPUS=$EXPECTED_GPUS"
echo "[$CURRENT_NODE] CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}"
echo "[$CURRENT_NODE] SPARK_LOCAL_DIRS=$SPARK_LOCAL_DIRS"

cleanup_spark() (
    set +e
    "${SPARK_HOME}/sbin/stop-worker.sh" >/dev/null 2>&1
    if [[ "${CURRENT_NODE}" == "${HEAD_NODE}" ]]; then
        "${SPARK_HOME}/sbin/stop-master.sh" >/dev/null 2>&1
    fi
    pkill -f "org.apache.spark.deploy.worker.Worker" >/dev/null 2>&1
    if [[ "${CURRENT_NODE}" == "${HEAD_NODE}" ]]; then
        pkill -f "org.apache.spark.deploy.master.Master" >/dev/null 2>&1
    fi
    true
)
trap cleanup_spark EXIT

cleanup_spark
sleep 2

if [[ "$CURRENT_NODE" == "$HEAD_NODE" ]]; then
    rm -f "$SPARK_MASTER_URL_FILE" "$DONE_FLAG" "$FAILED_FLAG"
    export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-$HEAD_NODE}
    echo "[$CURRENT_NODE] Starting Spark master at ${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
    "${SPARK_HOME}/sbin/start-master.sh" \
        --host "$SPARK_MASTER_HOST" \
        --port "$SPARK_MASTER_PORT" \
        --webui-port "$SPARK_MASTER_WEBUI_PORT"
    MASTER_URL="spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
    echo "$MASTER_URL" > "$SPARK_MASTER_URL_FILE"
else
    echo -n "[$CURRENT_NODE] Waiting for Spark master URL"
    set +x
    while [[ ! -f "$SPARK_MASTER_URL_FILE" ]]; do
        echo -n "."
        sleep 2
    done
    echo " found"
    set -x
fi

MASTER_URL=$(cat "$SPARK_MASTER_URL_FILE")
echo "[$CURRENT_NODE] Starting Spark worker against ${MASTER_URL}"
"${SPARK_HOME}/sbin/start-worker.sh" "$MASTER_URL" \
    --cores "$SPARK_WORKER_CORES" \
    --memory "$SPARK_WORKER_MEMORY" \
    --webui-port "$SPARK_WORKER_WEBUI_PORT"

if [[ "$CURRENT_NODE" == "$HEAD_NODE" ]]; then
    echo "[$CURRENT_NODE] Waiting for ${EXPECTED_WORKERS} Spark workers to register..."
    python3 - <<PY
import json
import sys
import time
import urllib.request

host = "${SPARK_MASTER_HOST:-$HEAD_NODE}"
port = int("${SPARK_MASTER_WEBUI_PORT}")
expected = int("${EXPECTED_WORKERS}")
deadline = time.time() + 300
last = None
while time.time() < deadline:
    try:
        with urllib.request.urlopen(f"http://{host}:{port}/json/", timeout=5) as response:
            data = json.load(response)
        workers = [w for w in data.get("workers", []) if w.get("state") == "ALIVE"]
        last = f"{len(workers)}/{expected} workers"
        print(f"  Spark workers registered: {last}", flush=True)
        if len(workers) >= expected:
            break
    except Exception as exc:
        last = repr(exc)
        print(f"  Waiting for Spark master JSON endpoint: {last}", flush=True)
    time.sleep(2)
else:
    print(f"Timed out waiting for Spark workers; last status: {last}", file=sys.stderr)
    sys.exit(1)
PY

    DATETIME=$(date +%Y-%m-%d_%H-%M-%S)
    output_log="$RESULTS_DIR/workflow-join-spark-1tb-rapids-${EXPECTED_GPUS}gpus-job-${SLURM_JOB_ID}-$DATETIME.txt"
    dataset_path=${DATASET_PATH:-/scratch/prestouser/test-data/500000-1TB}
    output_path=${OUTPUT_PATH:-${dataset_path}/workflow_join_spark_rapids_output}
    executor_instances=${SPARK_EXECUTOR_INSTANCES:-$EXPECTED_GPUS}
    executor_cores=${SPARK_EXECUTOR_CORES:-8}
    executor_memory=${SPARK_EXECUTOR_MEMORY:-96g}
    executor_memory_overhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD:-64g}
    driver_memory=${SPARK_DRIVER_MEMORY:-32g}
    shuffle_partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-2400}
    driver_host=${SPARK_DRIVER_HOST:-$HEAD_NODE}
    rapids_shuffle_manager=${SPARK_RAPIDS_SHUFFLE_MANAGER:-com.nvidia.spark.rapids.spark358.RapidsShuffleManager}

    if [[ "${SPARK_SMOKE_TEST:-0}" == "1" || "${SPARK_SMOKE_TEST:-false}" == "true" ]]; then
        smoke_py="$RESULTS_DIR/spark_rapids_smoke_job${SLURM_JOB_ID}-$DATETIME.py"
        cat > "$smoke_py" <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkRapidsSmoke").getOrCreate()
print("smoke_rapids_enabled", spark.conf.get("spark.rapids.sql.enabled", "unset"), flush=True)
print("smoke_executor_instances", spark.conf.get("spark.executor.instances", "unset"), flush=True)
result = (
    spark.range(0, 1_000_000)
    .selectExpr("id % 17 AS k", "id AS v")
    .groupBy("k")
    .count()
    .count()
)
print("smoke_result_rows", result, flush=True)
spark.stop()
PY
        echo "[$CURRENT_NODE] Running Spark RAPIDS smoke test" | tee "$output_log"
        set +e
        "${SPARK_HOME}/bin/spark-submit" \
            --master "$MASTER_URL" \
            --deploy-mode client \
            --driver-memory "$driver_memory" \
            --conf "spark.submit.deployMode=client" \
            --conf "spark.eventLog.enabled=true" \
            --conf "spark.eventLog.dir=$SPARK_EVENT_LOG_DIR" \
            --conf "spark.driver.bindAddress=0.0.0.0" \
            --conf "spark.driver.host=$driver_host" \
            --conf "spark.driver.extraClassPath=${SPARK_HOME}/sparkRapidsPlugin/rapids-4-spark.jar" \
            --conf "spark.executor.extraClassPath=${SPARK_HOME}/sparkRapidsPlugin/rapids-4-spark.jar" \
            --conf "spark.executor.instances=$executor_instances" \
            --conf "spark.executor.cores=$executor_cores" \
            --conf "spark.executor.memory=$executor_memory" \
            --conf "spark.executor.memoryOverhead=$executor_memory_overhead" \
            --conf "spark.executor.resource.gpu.amount=1" \
            --conf "spark.task.resource.gpu.amount=${SPARK_TASK_GPU_AMOUNT:-0.125}" \
            --conf "spark.executor.resource.gpu.vendor=nvidia.com" \
            --conf "spark.plugins=com.nvidia.spark.SQLPlugin" \
            --conf "spark.rapids.sql.enabled=true" \
            --conf "spark.rapids.allowMultipleJars=ALWAYS" \
            --conf "spark.rapids.sql.allowMultipleJars=ALWAYS" \
            --conf "spark.rapids.sql.explain=${SPARK_RAPIDS_SQL_EXPLAIN:-NONE}" \
            --conf "spark.rapids.memory.pinnedPool.size=${SPARK_RAPIDS_PINNED_POOL_SIZE:-4g}" \
            --conf "spark.rapids.memory.host.spillStorageSize=${SPARK_RAPIDS_HOST_SPILL_SIZE:-128G}" \
            --conf "spark.rapids.sql.concurrentGpuTasks=${SPARK_RAPIDS_CONCURRENT_GPU_TASKS:-1}" \
            --conf "spark.rapids.shuffle.mode=MULTITHREADED" \
            --conf "spark.shuffle.manager=$rapids_shuffle_manager" \
            --conf "spark.sql.shuffle.partitions=$shuffle_partitions" \
            "$smoke_py" \
            2>&1 | tee -a "$output_log"
        DRIVER_RC=${PIPESTATUS[0]}
        set -e

        if [[ "$DRIVER_RC" == "0" ]]; then
            touch "$DONE_FLAG"
        else
            touch "$FAILED_FLAG"
        fi
        exit "$DRIVER_RC"
    fi

    cp "$PROJECT_DIR/workflow_join_spark-1tb-rapids.py" "$RESULTS_DIR/workflow_join_spark-1tb-rapids-driver-job${SLURM_JOB_ID}-$DATETIME.py"
    cp "$PROJECT_DIR/workflow_join_spark-1tb-rapids.sh" "$RESULTS_DIR/workflow_join_spark-1tb-rapids-job${SLURM_JOB_ID}-$DATETIME.sh"
    cp "$PROJECT_DIR/workflow_join_spark-1tb-rapids.slurm" "$RESULTS_DIR/workflow_join_spark-1tb-rapids-job${SLURM_JOB_ID}-$DATETIME.slurm"

    driver_args=(
        --path-prefix "$dataset_path"
        --output "$output_path"
        --master "$MASTER_URL"
        --plugin "${SPARK_PLUGIN:-rapids}"
        --action "${WORKFLOW_ACTION:-parquet}"
        --mode "${SPARK_WRITE_MODE:-overwrite}"
        --event-log-dir "$SPARK_EVENT_LOG_DIR"
        --driver-host "$driver_host"
        --executor-instances "$executor_instances"
        --executor-cores "$executor_cores"
        --executor-memory "$executor_memory"
        --executor-memory-overhead "$executor_memory_overhead"
        --driver-memory "$driver_memory"
        --shuffle-partitions "$shuffle_partitions"
        --files-max-partition-bytes "${SPARK_SQL_FILES_MAX_PARTITION_BYTES:-160m}"
        --advisory-partition-size "${SPARK_SQL_ADVISORY_PARTITION_SIZE:-160mb}"
        --min-partition-size "${SPARK_SQL_MIN_PARTITION_SIZE:-32mb}"
        --rapids-concurrent-gpu-tasks "${SPARK_RAPIDS_CONCURRENT_GPU_TASKS:-1}"
        --task-gpu-amount "${SPARK_TASK_GPU_AMOUNT:-0.125}"
        --rapids-pinned-pool-size "${SPARK_RAPIDS_PINNED_POOL_SIZE:-4g}"
        --rapids-host-spill-size "${SPARK_RAPIDS_HOST_SPILL_SIZE:-128G}"
        --rapids-batch-size-bytes "${SPARK_RAPIDS_SQL_BATCH_SIZE_BYTES:-536870912b}"
        --rapids-shuffle-reader-threads "${SPARK_RAPIDS_SHUFFLE_READER_THREADS:-8}"
        --rapids-shuffle-writer-threads "${SPARK_RAPIDS_SHUFFLE_WRITER_THREADS:-8}"
        --rapids-shuffle-manager "$rapids_shuffle_manager"
        --rapids-explain "${SPARK_RAPIDS_SQL_EXPLAIN:-NONE}"
    )
    if [[ "${COUNT_INPUTS:-0}" == "1" || "${COUNT_INPUTS:-false}" == "true" ]]; then
        driver_args+=(--count-inputs)
    fi
    if [[ "${COUNT_RESULT:-0}" == "1" || "${COUNT_RESULT:-false}" == "true" ]]; then
        driver_args+=(--count-result)
    fi
    if [[ "${SPARK_EXPLAIN:-0}" == "1" || "${SPARK_EXPLAIN:-false}" == "true" ]]; then
        driver_args+=(--explain)
    fi
    if [[ "${OUTPUT_PARTITIONS:-0}" != "0" ]]; then
        driver_args+=(--output-partitions "${OUTPUT_PARTITIONS}")
    fi

    echo "[$CURRENT_NODE] Submitting Spark RAPIDS workflow join" | tee "$output_log"
    set +e
    "${SPARK_HOME}/bin/spark-submit" \
        --master "$MASTER_URL" \
        --deploy-mode client \
        --driver-memory "$driver_memory" \
        --conf "spark.submit.deployMode=client" \
        --conf "spark.eventLog.enabled=true" \
        --conf "spark.eventLog.dir=$SPARK_EVENT_LOG_DIR" \
        --conf "spark.driver.bindAddress=0.0.0.0" \
        --conf "spark.driver.host=$driver_host" \
        --conf "spark.driver.extraClassPath=${SPARK_HOME}/sparkRapidsPlugin/rapids-4-spark.jar" \
        --conf "spark.executor.extraClassPath=${SPARK_HOME}/sparkRapidsPlugin/rapids-4-spark.jar" \
        --conf "spark.executor.instances=$executor_instances" \
        --conf "spark.executor.cores=$executor_cores" \
        --conf "spark.executor.memory=$executor_memory" \
        --conf "spark.executor.memoryOverhead=$executor_memory_overhead" \
        --conf "spark.executor.resource.gpu.amount=1" \
        --conf "spark.task.resource.gpu.amount=${SPARK_TASK_GPU_AMOUNT:-0.125}" \
        --conf "spark.executor.resource.gpu.vendor=nvidia.com" \
        --conf "spark.plugins=com.nvidia.spark.SQLPlugin" \
        --conf "spark.rapids.sql.enabled=true" \
        --conf "spark.rapids.sql.explain=${SPARK_RAPIDS_SQL_EXPLAIN:-NONE}" \
        --conf "spark.rapids.memory.pinnedPool.size=${SPARK_RAPIDS_PINNED_POOL_SIZE:-4g}" \
        --conf "spark.rapids.memory.host.spillStorageSize=${SPARK_RAPIDS_HOST_SPILL_SIZE:-128G}" \
        --conf "spark.rapids.sql.batchSizeBytes=${SPARK_RAPIDS_SQL_BATCH_SIZE_BYTES:-536870912b}" \
        --conf "spark.rapids.sql.concurrentGpuTasks=${SPARK_RAPIDS_CONCURRENT_GPU_TASKS:-1}" \
        --conf "spark.rapids.shuffle.mode=MULTITHREADED" \
        --conf "spark.rapids.shuffle.multiThreaded.reader.threads=${SPARK_RAPIDS_SHUFFLE_READER_THREADS:-8}" \
        --conf "spark.rapids.shuffle.multiThreaded.writer.threads=${SPARK_RAPIDS_SHUFFLE_WRITER_THREADS:-8}" \
        --conf "spark.shuffle.manager=$rapids_shuffle_manager" \
        --conf "spark.shuffle.compress=true" \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.adaptive.coalescePartitions.parallelismFirst=false" \
        --conf "spark.sql.adaptive.skewJoin.enabled=true" \
        --conf "spark.sql.shuffle.partitions=$shuffle_partitions" \
        --conf "spark.sql.files.maxPartitionBytes=${SPARK_SQL_FILES_MAX_PARTITION_BYTES:-160m}" \
        --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=${SPARK_SQL_ADVISORY_PARTITION_SIZE:-160mb}" \
        --conf "spark.sql.adaptive.coalescePartitions.minPartitionSize=${SPARK_SQL_MIN_PARTITION_SIZE:-32mb}" \
        "$PROJECT_DIR/workflow_join_spark-1tb-rapids.py" \
        "${driver_args[@]}" \
        2>&1 | tee -a "$output_log"
    DRIVER_RC=${PIPESTATUS[0]}
    set -e

    if [[ "$DRIVER_RC" == "0" ]]; then
        touch "$DONE_FLAG"
    else
        touch "$FAILED_FLAG"
    fi
    exit "$DRIVER_RC"
else
    while [[ ! -f "$DONE_FLAG" && ! -f "$FAILED_FLAG" ]]; do
        sleep 5
    done
    if [[ -f "$FAILED_FLAG" ]]; then
        exit 1
    fi
fi

echo "[$CURRENT_NODE] Exiting."
