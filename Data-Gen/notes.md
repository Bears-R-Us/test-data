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
