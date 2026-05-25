1. new_datagen_faster.py
2. create_join_tables.py

new_datagen_faster.py runs first. It generates the base table_a data by:

Creating synthetic graph partitions using a power-law degree distribution (via networkx configuration models)
Writing the initial edge data as parquet to PATH_PREFIX
Scaling it up to the target size (1 GB) by cross-joining copies with noise
Then create_join_tables.py runs second. It:

Reads the table_a parquet that new_datagen_faster.py produced (line 37: table_a = spark.read.parquet(PATH_PREFIX))
Generates the other tables (table_b, table_c, table_d, table_e) from schema metadata in an Excel file
Forces join-key alignment by overwriting a percentage of rows in each table with values from table_b


## 50 GB
target_sze = 50 * 1024**3 # 1 GB
num_nodes_per_graph is still 500_000 

### Spark CPU

workflow_join-50gb.py changes the config.  But I doubt this is optimal.  Running on `2xGrace` (144 cores)
.config("spark.driver.memory", "128g")
.config("spark.memory.fraction", "0.8")
.config("spark.sql.shuffle.partitions", "800")

```
Workflow join completed in 1235.04 seconds
  Output: /scratch/prestouser/test-data/500000-50GB/workflow_join_spark_output
  Result rows: 2662631628
```

### cuDF-Polars
workflow_join_polars-50gb.py Running on full NVL4: 2xGrace 4xB200 

workflow_join_polars-50gb.py

```
Workflow join completed in 43.47 seconds
  Output: /scratch/prestouser/test-data/500000-50GB/workflow_join_polars_output
  Result rows: 2662631628

RapidsMPF statistics:
Statistics:
 - alloc-device:                353.05 GiB | 1.63 s | 216.76 GiB/s | avg-stream-delay 2.02 ms
 - alloc-host:                  1.98 KiB | 270.61 us | 7.14 MiB/s | avg-stream-delay 97.29 us
 - copy-device-to-pinned_host:  322.07 GiB | 2.94 s | 109.71 GiB/s | avg-stream-delay 996.55 us
 - copy-pinned_host-to-device:  322.07 GiB | 1.71 s | 188.09 GiB/s | avg-stream-delay 26.02 us
```


## 1GB

target_size = 1 * 1024**3 # 1 GB
num_nodes_per_graph = 500_000
PATH_PREFIX = f"/scratch/prestouser/test-data/{num_nodes_per_graph}-1GB"

Reading data from: /scratch/prestouser/test-data/500000-1GB
Creating 1 versions in parallel...
Writing combined dataset with 10 partitions...
Estimated final size: 2.68 GB
                                                                                                                                                                Final size: 2.76 GB
Target was: 1.0 GB
Achieved: 276.0% of target


1. Processing table_a...
   Distinct combinations for table_a: 14000
   ✓ table_a_forced created (0.1% forced)

2. Processing table_c...
   Distinct combinations for table_c: 14000
   ✓ table_c_forced created (1.0% forced)

3. Processing table_d...
   Distinct combinations for table_d: 14000
   ✓ table_d_forced created (1.0% forced)

4. Processing table_e...
   Distinct values for table_e: 14000
   ✓ table_e_forced created (1.0% forced)



## 10GB
target_size = 10 * 1024**3 # 1 GB
num_nodes_per_graph = 500_000
PATH_PREFIX = f"/scratch/prestouser/test-data/{num_nodes_per_graph}-1GB"

Reading data from: /scratch/prestouser/test-data/500000-1GB
Creating 4 versions in parallel...
Writing combined dataset with 43 partitions...
Estimated final size: 10.7 GB

Final size: 11.04 GB
Target was: 10.0 GB
Achieved: 110.4% of target

Created DataFrame for table 'table_b' with 14000 random rows.
Created DataFrame for table 'table_c' with 21000000 random rows.
Created DataFrame for table 'table_d' with 12000000 random rows.
Created DataFrame for table 'table_e' with 33000000 random rows.
============================================================
FORCING TABLES TO MATCH TABLE_B VALUES
Match percentages: A=0.1%, C=1.0%, D=1.0%, E=1.0%
============================================================

1. Processing table_a...
   Distinct combinations for table_a: 14000
   ✓ table_a_forced created (0.1% forced)

2. Processing table_c...
   Distinct combinations for table_c: 14000
   ✓ table_c_forced created (1.0% forced)

3. Processing table_d...
   Distinct combinations for table_d: 14000
   ✓ table_d_forced created (1.0% forced)

4. Processing table_e...
   Distinct values for table_e: 14000
   ✓ table_e_forced created (1.0% forced)
