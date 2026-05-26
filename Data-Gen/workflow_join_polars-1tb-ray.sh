#!/bin/bash
# Multi-node Ray launcher for workflow_join_polars-1tb-ray.py.
set -x
set -e

export UCX_TLS=^ib,ud:aux
export UCX_NET_DEVICES=bond0
export UCX_MAX_RNDV_RAILS=1
export UCX_RNDV_PIPELINE_ERROR_HANDLING=y
export UCX_TCP_CM_REUSEADDR=y
export UCX_RNDV_MTYPE_WORKER_MAX_MEM=1G
export UCX_RNDV_MTYPE_WORKER_FC_ENABLE=y
export UCX_RNDV_FRAG_MEM_TYPES=cuda

export PYTHONUNBUFFERED=1
export POLARS_MAX_THREADS=1
export RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0
export CUDF_POLARS_LOG_TRACES=1
export KVIKIO_TASK_SIZE=$((16 * 1024 * 1024))
export CUFILE_LOGGING_LEVEL=INFO
export KVIKIO_COMPAT_MODE=OFF

source /opt/conda/etc/profile.d/conda.sh
conda activate rapidsmpf

PROJECT_DIR=${REAL_HOME:-$HOME}/bzaitlen/test-data/Data-Gen
SCHEDULER_DIR=${PROJECT_DIR}/scheduler
RESULTS_DIR=${PROJECT_DIR}/results
RAY_ADDR_FILE=${SCHEDULER_DIR}/ray-address-job${SLURM_JOB_ID}.txt
DONE_FLAG=${SCHEDULER_DIR}/ray-done-job${SLURM_JOB_ID}.flag
mkdir -p "$SCHEDULER_DIR" "$RESULTS_DIR"

CURRENT_NODE=$(hostname -s)
echo "Node: $CURRENT_NODE  Head: $HEAD_NODE  NodeID: $SLURM_NODEID  Procid: $SLURM_PROCID"

NODE_GPUS=${SLURM_GPUS_ON_NODE:-4}
EXPECTED_GPUS=$((NODE_GPUS * SLURM_NNODES))
echo "[$CURRENT_NODE] NODE_GPUS=$NODE_GPUS EXPECTED_GPUS=$EXPECTED_GPUS"
echo "[$CURRENT_NODE] CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}"

HEAD_PORT=${RAY_HEAD_PORT:-6379}
RAY_TMP_DIR=/tmp/ray-${SLURM_JOB_ID}

ray stop --force 2>/dev/null || true
killall -q raylet gcs_server 2>/dev/null || true
pkill -f "ray::" 2>/dev/null || true
rm -rf "$RAY_TMP_DIR" 2>/dev/null || true
sleep 2

if [[ "$CURRENT_NODE" == "$HEAD_NODE" ]]; then
    rm -f "$RAY_ADDR_FILE" "$DONE_FLAG"
    HEAD_IP=$(hostname -I | awk '{print $1}')
    echo "[$CURRENT_NODE] Starting Ray head at ${HEAD_IP}:${HEAD_PORT}"

    ray start --head \
        --node-ip-address="$HEAD_IP" \
        --port="$HEAD_PORT" \
        --num-gpus="$NODE_GPUS" \
        --temp-dir="$RAY_TMP_DIR" \
        --disable-usage-stats

    echo "${HEAD_IP}:${HEAD_PORT}" > "$RAY_ADDR_FILE"

    echo "[$CURRENT_NODE] Waiting for $EXPECTED_GPUS GPUs to join cluster..."
    python - <<PY
import time, sys, ray
ray.init(address="${HEAD_IP}:${HEAD_PORT}", ignore_reinit_error=True)
deadline = time.time() + 600
while time.time() < deadline:
    have = int(ray.cluster_resources().get("GPU", 0))
    print(f"  GPUs in cluster: {have}/${EXPECTED_GPUS}", flush=True)
    if have >= ${EXPECTED_GPUS}:
        break
    time.sleep(2)
else:
    print("Timed out waiting for GPUs", file=sys.stderr)
    sys.exit(1)
ray.shutdown()
PY

    DATETIME=$(date +%Y-%m-%d_%H-%M-%S)
    output_log="$RESULTS_DIR/workflow-join-polars-1tb-ray-${EXPECTED_GPUS}gpus-job-${SLURM_JOB_ID}-$DATETIME.txt"
    dataset_path=${DATASET_PATH:-/scratch/prestouser/test-data/500000-1TB}
    output_path=${OUTPUT_PATH:-${dataset_path}/workflow_join_polars_ray_output}

    cp "$PROJECT_DIR/workflow_join_polars-1tb-ray.py" "$RESULTS_DIR/workflow_join_polars-1tb-ray-driver-job${SLURM_JOB_ID}-$DATETIME.py"
    cp "$PROJECT_DIR/workflow_join_polars-1tb-ray.sh" "$RESULTS_DIR/workflow_join_polars-1tb-ray-job${SLURM_JOB_ID}-$DATETIME.sh"
    cp "$PROJECT_DIR/workflow_join_polars-1tb-ray.slurm" "$RESULTS_DIR/workflow_join_polars-1tb-ray-job${SLURM_JOB_ID}-$DATETIME.slurm"

    set +e
    python "$PROJECT_DIR/workflow_join_polars-1tb-ray.py" \
        --path-prefix "$dataset_path" \
        --output "$output_path" \
        --ray-address "${HEAD_IP}:${HEAD_PORT}" \
        --num-py-executors "${NUM_PY_EXECUTORS:-8}" \
        --target-partition-size "${TARGET_PARTITION_SIZE:-3221225472}" \
        --spill-device-limit "${SPILL_DEVICE_LIMIT:-70%}" \
        2>&1 | tee -a "$output_log"
    DRIVER_RC=${PIPESTATUS[0]}
    set -e

    touch "$DONE_FLAG"
    ray stop --force || true
    rm -f "$RAY_ADDR_FILE"
    exit "$DRIVER_RC"
else
    echo -n "[$CURRENT_NODE] Worker waiting for Ray address"
    set +x
    while [ ! -f "$RAY_ADDR_FILE" ]; do
        echo -n "."
        sleep 2
    done
    echo " found"
    set -x

    HEAD_ADDR=$(cat "$RAY_ADDR_FILE")
    echo "[$CURRENT_NODE] Joining Ray at ${HEAD_ADDR} with $NODE_GPUS GPUs"
    ray start \
        --address="$HEAD_ADDR" \
        --num-gpus="$NODE_GPUS" \
        --temp-dir="$RAY_TMP_DIR" \
        --disable-usage-stats

    while [ ! -f "$DONE_FLAG" ]; do
        sleep 5
    done
    ray stop --force || true
fi

echo "[$CURRENT_NODE] Exiting."
