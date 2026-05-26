#!/bin/bash
#SBATCH --job-name=bz-workflow-join-polars-1tb-ray
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --time=00:40:00
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH --gres=gpu:4
#SBATCH --exclusive
#SBATCH --nodelist=presto-gb200-gcn-[01-10]

export DATASET_PATH=${DATASET_PATH:-/scratch/prestouser/test-data/500000-1TB}
export OUTPUT_PATH=${OUTPUT_PATH:-${DATASET_PATH}/workflow_join_polars_ray_output}

export LIBCUDF_HW_DECOMPRESSION=${LIBCUDF_HW_DECOMPRESSION:-OFF}
export NUM_PY_EXECUTORS=${NUM_PY_EXECUTORS:-8}
export TARGET_PARTITION_SIZE=${TARGET_PARTITION_SIZE:-3221225472}
export SPILL_DEVICE_LIMIT=${SPILL_DEVICE_LIMIT:-70%}

export NVIDIA_VISIBLE_DEVICES=all
export NVIDIA_DRIVER_CAPABILITIES=compute,utility
export REAL_HOME=$HOME

LOCAL_CONTAINER_PATH=${LOCAL_CONTAINER_PATH:-/scratch/prestouser/images/mg-polars-tpch-nvl72-2026-05-23-arm64-cuda-13-1-ucxthrottling-nvcr.sqsh}
CONTAINER_MOUNTS="${HOME}:${HOME},/scratch:/scratch"

export HEAD_NODE=$(scontrol show hostnames $SLURM_JOB_NODELIST | head -n 1)

mkdir -p logs ${HOME}/bzaitlen/test-data/Data-Gen/scheduler

echo "========================================"
echo "Job ID:           $SLURM_JOB_ID"
echo "Nodes:            $SLURM_JOB_NUM_NODES"
echo "Node list:        $SLURM_JOB_NODELIST"
echo "Head node:        $HEAD_NODE"
echo "Dataset:          $DATASET_PATH"
echo "Output:           $OUTPUT_PATH"
echo "HW decompression: $LIBCUDF_HW_DECOMPRESSION"
echo "Frontend:         ray"
echo "Allocated GPUs:   ${SLURM_GPUS_ON_NODE:-4} per node"
echo "Target partition: $TARGET_PARTITION_SIZE"
echo "========================================"

srun \
    --export=ALL \
    --container-image=${LOCAL_CONTAINER_PATH} \
    --container-mounts=${CONTAINER_MOUNTS} \
    --container-remap-root \
    --container-writable \
    --pty \
    bash ${HOME}/bzaitlen/test-data/Data-Gen/workflow_join_polars-1tb-ray.sh
