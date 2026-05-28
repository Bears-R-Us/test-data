#!/bin/bash
#SBATCH --job-name=bz-workflow-join-spark-1tb-rapids
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH --gres=gpu:4
#SBATCH --exclusive
#SBATCH --nodelist=presto-gb200-gcn-[01-16]

export DATASET_PATH=${DATASET_PATH:-/scratch/prestouser/test-data/500000-1TB}
export OUTPUT_PATH=${OUTPUT_PATH:-${DATASET_PATH}/workflow_join_spark_rapids_output}
export WORKFLOW_ACTION=${WORKFLOW_ACTION:-parquet}

export SPARK_PLUGIN=${SPARK_PLUGIN:-rapids}
export SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-8}
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-96g}
export SPARK_EXECUTOR_MEMORY_OVERHEAD=${SPARK_EXECUTOR_MEMORY_OVERHEAD:-64g}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-32g}
export SPARK_SQL_SHUFFLE_PARTITIONS=${SPARK_SQL_SHUFFLE_PARTITIONS:-2400}
export SPARK_SQL_FILES_MAX_PARTITION_BYTES=${SPARK_SQL_FILES_MAX_PARTITION_BYTES:-160m}
export SPARK_RAPIDS_CONCURRENT_GPU_TASKS=${SPARK_RAPIDS_CONCURRENT_GPU_TASKS:-1}
export SPARK_TASK_GPU_AMOUNT=${SPARK_TASK_GPU_AMOUNT:-0.125}
export SPARK_RAPIDS_PINNED_POOL_SIZE=${SPARK_RAPIDS_PINNED_POOL_SIZE:-4g}
export SPARK_RAPIDS_HOST_SPILL_SIZE=${SPARK_RAPIDS_HOST_SPILL_SIZE:-128G}
export SPARK_RAPIDS_SQL_BATCH_SIZE_BYTES=${SPARK_RAPIDS_SQL_BATCH_SIZE_BYTES:-536870912b}
export SPARK_RAPIDS_SHUFFLE_READER_THREADS=${SPARK_RAPIDS_SHUFFLE_READER_THREADS:-8}
export SPARK_RAPIDS_SHUFFLE_WRITER_THREADS=${SPARK_RAPIDS_SHUFFLE_WRITER_THREADS:-8}
export SPARK_RAPIDS_SHUFFLE_MANAGER=${SPARK_RAPIDS_SHUFFLE_MANAGER:-com.nvidia.spark.rapids.spark358.RapidsShuffleManager}

export COUNT_INPUTS=${COUNT_INPUTS:-0}
export COUNT_RESULT=${COUNT_RESULT:-0}
export OUTPUT_PARTITIONS=${OUTPUT_PARTITIONS:-0}
export SPARK_SMOKE_TEST=${SPARK_SMOKE_TEST:-0}

export NVIDIA_VISIBLE_DEVICES=all
export NVIDIA_DRIVER_CAPABILITIES=compute,utility
export REAL_HOME=$HOME

LOCAL_CONTAINER_PATH=${LOCAL_CONTAINER_PATH:-/scratch/prestouser/images/test-data-spark-rapids-2026-05-28_19-03-04-arm64-cuda13-spark3.5.8-rapids26.04.2.sqsh}
CONTAINER_MOUNTS="${HOME}:${HOME},/scratch:/scratch"

export HEAD_NODE=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
export SPARK_MASTER_URL=spark://${HEAD_NODE}:${SPARK_MASTER_PORT:-7077}
export SPARK_DRIVER_HOST=${SPARK_DRIVER_HOST:-$HEAD_NODE}
export SPARK_EXECUTOR_INSTANCES=${SPARK_EXECUTOR_INSTANCES:-$((SLURM_JOB_NUM_NODES * 4))}

mkdir -p logs ${HOME}/bzaitlen/test-data/Data-Gen/scheduler

echo "========================================"
echo "Job ID:             $SLURM_JOB_ID"
echo "Nodes:              $SLURM_JOB_NUM_NODES"
echo "Node list:          $SLURM_JOB_NODELIST"
echo "Head node:          $HEAD_NODE"
echo "Dataset:            $DATASET_PATH"
echo "Output:             $OUTPUT_PATH"
echo "Action:             $WORKFLOW_ACTION"
echo "Spark plugin:       $SPARK_PLUGIN"
echo "Container:          $LOCAL_CONTAINER_PATH"
echo "Executor instances: $SPARK_EXECUTOR_INSTANCES"
echo "Executor cores:     $SPARK_EXECUTOR_CORES"
echo "Executor memory:    $SPARK_EXECUTOR_MEMORY"
echo "Shuffle partitions: $SPARK_SQL_SHUFFLE_PARTITIONS"
echo "RAPIDS pinned pool: $SPARK_RAPIDS_PINNED_POOL_SIZE"
echo "RAPIDS host spill:  $SPARK_RAPIDS_HOST_SPILL_SIZE"
echo "RAPIDS batch bytes: $SPARK_RAPIDS_SQL_BATCH_SIZE_BYTES"
echo "RAPIDS shuffle mgr: $SPARK_RAPIDS_SHUFFLE_MANAGER"
echo "Smoke test:         $SPARK_SMOKE_TEST"
echo "========================================"

srun \
    --export=ALL \
    --container-image=${LOCAL_CONTAINER_PATH} \
    --container-mounts=${CONTAINER_MOUNTS} \
    --container-remap-root \
    --container-writable \
    bash ${HOME}/bzaitlen/test-data/Data-Gen/workflow_join_spark-1tb-rapids.sh
