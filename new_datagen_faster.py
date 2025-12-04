import random
import subprocess

import networkx as nx
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Row

# spark = SparkSession.builder.master("local[*]").appName("LargeGraph").getOrCreate()
def create_synthetic_distribution(params, plot=True):

	slope = params.get('slope', -2)
	min_degree = params.get('min_degree', 1)
	max_degree = params.get('max_degree', 200_000)
	max_prob = params.get('max_prob', 0.5)
	
	degrees = np.arange(min_degree, max_degree + 1, dtype=float)
	
	A = max_prob / (min_degree ** slope)
	
	y_values = A * degrees ** slope
	
	degrees_int = degrees.astype(int)
	
	decay_dict = dict(zip(degrees_int, y_values))
	
	return decay_dict
	
params = {
	'slope': -2,
	'intercpet': 0.8,
	'r_squared': 0.98,
	'max_degree': 200_000,
	'min_degree': 1,
	'max_prob': 0.5,
	'degree_range': list(np.arange(1, 200_000))
}

target_distribution = create_synthetic_distribution(params, 200_000)

def get_disk_usage(path):
    try:
        result = subprocess.run(
            ['du', '-sb', path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        size_in_bytes = int(result.stdout.split()[0])
        return size_in_bytes
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to get disk usage: {e.stderr.strip()}")

def random_node():
    return int(np.random.randint(1_000_000, 10_000_000_000))

def random_feature():
    return int(np.random.randint(1, 70000))  # cast to native int

def random_col_e():
    return str(np.random.choice(['col_e_A', 'col_e_B']))  # cast to native str

num_graphs = spark.sparkContext.defaultParallelism # number of cores available
num_nodes_per_graph = 500_000

def configuration_model_with_distribution(n, degree_distribution,seed):
    """
    Generate a graph with a specific degree distribution
    """
    degrees = []
    remaining_nodes = n

    for degree, prob in sorted(degree_distribution.items()):
        if remaining_nodes <= 0:
            break
        count = min(int(n * prob + 0.5), remaining_nodes)
        if count > 0:
            degrees.extend([int(degree)] * count)
            remaining_nodes -= count

    if remaining_nodes > 0:
        min_degree = min(degree_distribution.keys())
        degrees.extend([min_degree] * remaining_nodes)

    if len(degrees) < 2:
        degrees = [1, 1]

    if sum(degrees) % 2 != 0:
        degrees[0] += 1

    try:
        g = nx.configuration_model(degrees, seed=seed)
        g = nx.Graph(g)

        if g.number_of_edges() == 0:
            raise nx.NetworkXError("Generated graph has no edges")

        return g
    except Exception as e:
        print(f"Error generating graph: {e}")
        return nx.barabasi_ablert_graph(n, 2)
    
def generate_graph_partition(pdf_iterator):
	"""
	Generator function that yeilds edges for each partition.
	pdf_iterator yields pandas DataFrames (one per partition)
	"""
	# Get broadcasted values
	degree_dist = target_distribution_bc.value
	seed_val = seed_bc.value
	
	for pdf in pdf_iterator:
		all_edges = []
		
		for partition_id in pdf['id'].values:
			partition_id = int(partition_id)
			
			g = configuration_model_with_distribution(
				num_nodes_per_graph,
				degree_dist,
				seed_val + partition_id
			)
			
			node_map = {node: random_node() for node in g.nodes()}
			
			for edge in g.edges():
				all_edges.append({
					'col_a': int(node_map[edge[0]]),
					'col_b': int(node_map[edge[1]]),
					'col_c': int(random_feature()),
					'col_d': int(random_feature()),
					'col_e': random_col_e()
				})
				
			if all_edges:
				yield pd.DataFrame(all_edges)

target_distribution_bc = spark.sparkContext.broadcast(target_distribution)
directory_path = "/path/to/synthetic_dataset"
seed = 1000
seed_bc = spark.sparkContext.broadcast(seed)

edge_df = (
	spark.range(num_graphs)
		.repartition(num_graphs)
		.mapInPandas(
			generate_graph_partition,
			schema="col_a long, col_b long, col_c int, col_d int, col_e string"
		)
		.distinct()
)

edge_df.write.mode("overwrite").parquet(directory_path)

print(f"Initial write complete. Size: {get_disk_usage(directory_path) / 1024**3:.2f} GB")


# =======================================================================================
# ================================ REVAMPED DATA SCALER =================================
# =======================================================================================

from pyspark.sql.functions import col, floor, rand, lit, when, has as spark_hash
import math

target_size = 8 * 1024**4 # 8 TB
initial_dir_size = get_disk_usage(directory_path)

print(f"Initial dataset size: {round(initial_dir_size / 1024**3, 2)} GB")

copies_needed = math.ceil(target_size / initial_dir_size)

print(f"Target size: {round(target_size / 1024**3, 2)} GB")
print(f"Copies needed (including original): {copies_needed}")

long_cols = ["col_a", "col_b"]
integer_cols = ["col_c", "col_d"]
string_cols = ["col_e"]

print(f"Reading data from: {directory_path}")
df_original = spark.read.parquet(directory_path)

if 'source' in df_original.columns:
	df_original = df_original.drop('source')
	
# Create a "copy_id" Dataframe
df_copies = spark.range(copies_needed).toDF("copy_id")

# Cross join to create all copies at once
df_expanded = df_original.crossJoin(df_copies)

print(f"Creating {copies_needed} versions in parallel...")

# Define noise range
NOISE_MIN = -1
NOISE_MAX = 1

# Create a seed column based on copy_id and row hash
# This gives us different randomness for each day
df_with_seed = df_expanded.withColumn(
	"rand_seed",
	(spark_hash(col("col_a"), col("col_b"), col("copy_id")) % 1000000).cast("integer")
)

# Apply noise based on copy_id (copy_id=0 is orignal, no noise)
# Use rand() with a base seed, then add variation based on the row's values
df_augmented = df_with_seed.select(
	*[
        when(col("copy_id") == 0, col(c)).otherwise(
			(col(c) + floor(rand(42) * (NOISE_MAX - NOISE_MIN + 1)) + NOISE_MIN)
		).cast("long").alias(c)
		for c in long_cols
	],
	*[
		when(col("copy_id") == 0, col(c)).otherwise(
			(col(c) + floor(rand(1042) * (NOISE_MAX - NOISE_MIN + 1)) + NOISE_MIN)
		).cast("integer").alias(c)
		for c in integer_cols
	],
	*[col(c) for c in string_cols]
)

# Calculate partitions based on target file size
target_file_size_mb = 250
estimated_size = initial_dir_size * copies_needed
repartitions = max(int(estimated_size / (target_file_size_mb / 1024**3, 2)), " GB")

print(f"Writing combined dataset with {repartitions} partitions...")
print(f"Estimated final size: {round(estimated_size / 1024**3, 2)} GB")

# Write everytihing in one shot
df_augmented.repartition(repartitions).write.mode("overwrite").parquet(directory_path)

# Verify final size
final_size = get_disk_usage(directory_path)
print(f"\nFinal size: {round(final_size / 1024**3, 2)} GB")
print(f"Target was: {round(target_size / 1024**3, 2)} GB")
print(f"Achieved: {round(100 * final_size / target_size, 1)}% of target")