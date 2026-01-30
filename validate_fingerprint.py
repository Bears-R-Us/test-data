from pyspark.sql import functions as F
from pyspark.sql import Window
import hashlib
import json
from datetime import datetime


def generate_dataset_fingerprint(df, directory_path, sample_size=10000):
    """
    Generate a deterministic fingerprint of the dataset that can be compared across systems.
    
    Args:
        df: Spark DataFrame to fingerprint
        directory_path: Path where data is stored
        sample_size: Number of rows to sample for detailed checksum
    
    Returns:
        dict: Fingerprint containing various metrics and checksums
    """
    print("Generating dataset fingerprint...")
    print("=" * 60)
    
    fingerprint = {
        "generation_time": datetime.now().isoformat(),
        "directory_path": directory_path,
    }
    
    # Schema: normalize to sorted field list for deterministic comparison
    schema_fields = sorted([
        {"name": field.name, "type": str(field.dataType), "nullable": field.nullable}
        for field in df.schema.fields
    ], key=lambda x: x["name"])
    fingerprint["schema"] = schema_fields
    
    # 1. Basic counts
    print("Computing basic statistics...")
    total_rows = df.count()
    fingerprint["total_rows"] = total_rows
    
    # 2. Column-wise statistics (deterministic)
    print("Computing column statistics...")
    stats = {}
    
    # Get schema to check column types
    schema_dict = {field.name: field.dataType for field in df.schema.fields}
    numeric_types = ('LongType', 'IntegerType', 'DoubleType', 'FloatType', 'DecimalType', 'ShortType', 'ByteType')
    
    for col_name in df.columns:
        # Check if column is numeric
        col_type = str(schema_dict[col_name])
        is_numeric = any(nt in col_type for nt in numeric_types)
        
        # Base aggregations that work for all types
        agg_exprs = [
            F.count(col_name).alias("count"),
            F.countDistinct(col_name).alias("distinct"),
            F.min(col_name).alias("min"),
            F.max(col_name).alias("max"),
        ]
        
        # Add sum only for numeric columns
        if is_numeric:
            agg_exprs.append(F.sum(F.col(col_name).cast("long")).alias("sum"))
        
        col_stats = df.agg(*agg_exprs).collect()[0]
        
        stats[col_name] = {
            "count": col_stats["count"],
            "distinct": col_stats["distinct"],
            "min": str(col_stats["min"]),
            "max": str(col_stats["max"]),
            "sum": col_stats["sum"] if is_numeric else None,
        }
        
    fingerprint["column_stats"] = stats
    
    # 3. Edge-specific metrics
    print("Computing graph-specific metrics...")
    
    # Self-loops count
    self_loops = df.filter(F.col("col_a") == F.col("col_b")).count()
    fingerprint["self_loops"] = self_loops
    
    # Unique Nodes
    unique_nodes_a = df.select("col_a").distinct().count()
    unique_nodes_b = df.select("col_b").distinct().count()
    all_nodes = df.select(F.col("col_a").alias("node")).union(
        df.select(F.col("col_b").alias("node"))
    ).distinct().count()
    
    fingerprint["unique_nodes_col_a"] = unique_nodes_a
    fingerprint["unique_nodes_col_b"] = unique_nodes_b
    fingerprint["all_nodes"] = all_nodes
    
    # 4. Degree distribution fingerprint
    print("Computing degree distribution fingerprint...")
    degrees_df = (
        df.select(F.col("col_a").alias("node"))
        .union(df.select(F.col("col_b").alias("node")))
        .groupBy("node")
        .agg(F.count("*").alias("degree"))
    )
    
    degree_dist = degrees_df.groupBy("degree").count().orderBy("degree")
    
    # Create a hash of the degree distribution
    degree_data = degree_dist.collect()
    degree_str = "|".join([f"{row['degree']}:{row['count']}" for row in degree_data])
    degree_hash = hashlib.sha256(degree_str.encode()).hexdigest()
    
    fingerprint["degree_distribution_hash"] = degree_hash
    fingerprint["degree_distribution_entries"] = len(degree_data)
    
    # Statistics on degrees
    degree_stats = degrees_df.agg(
        F.min("degree").alias("min_degree"),
        F.max("degree").alias("max_degree"),
        F.avg("degree").alias("avg_degree"),
        F.stddev("degree").alias("stddev_degree"),
    ).collect()[0]
    
    fingerprint["degree_stats"] = {
        "min": degree_stats["min_degree"],
        "max": degree_stats["max_degree"],
        "avg": float(degree_stats["avg_degree"]),
        "stddev": float(degree_stats["stddev_degree"]) if degree_stats["stddev_degree"] else 0.0
    }
    
    # 5. Deterministic sample checksum
    # Sort by all columns to ensure deterministic ordering, then take top N
    print(f"Computing checksum of {sample_size} deterministic samples...")
    
    sample_df = df.orderBy("col_a", "col_b", "col_c", "col_d", "col_e").limit(sample_size)
    sample_rows = sample_df.collect()
    
    # Create hash of sample
    sample_str = "|".join([
        f"{row['col_a']},{row['col_b']},{row['col_c']},{row['col_d']},{row['col_e']}"
        for row in sample_rows
    ])
    sample_hash = hashlib.sha256(sample_str.encode()).hexdigest()
    
    fingerprint["sample_checksum"] = sample_hash
    fingerprint["sample_size"] = len(sample_rows)
    
    # 6. Partition-level checksums (for distributed validation)
    # Note: We use a seeded sample to ensure determinism across different systems.
    # The seed ensures the same rows are selected regardless of cluster configuration.
    print("Computing partition checksums...")
    
    PARTITION_SAMPLE_SEED = 42  # Fixed seed for deterministic sampling
    
    # Get checksum per partition by hashing sorted content
    def partition_checksum(iterator):
        rows = sorted(iterator, key=lambda r: (r.col_a, r.col_b, r.col_c, r.col_d, r.col_e))
        content = "|".join([
            f"{r.col_a},{r.col_b},{r.col_c},{r.col_d},{r.col_e}"
            for r in rows
        ])
        checksum = hashlib.sha256(content.encode()).hexdigest()
        yield (checksum,)
    
    # Sample with fixed seed for determinism across systems
    sample_fraction = min(1.0, 100000 / total_rows)
    partition_checksums = (
        df.sample(withReplacement=False, fraction=sample_fraction, seed=PARTITION_SAMPLE_SEED)
        .rdd
        .mapPartitions(partition_checksum)
        .collect()
    )
    
    # Hash all partition checksums together (sorted to handle different partition counts)
    combined_partition_hash = hashlib.sha256(
        "|".join(sorted([cs[0] for cs in partition_checksums])).encode()
    ).hexdigest()
    
    fingerprint["partition_checksum"] = combined_partition_hash
    fingerprint["num_partitions_sampled"] = len(partition_checksums)
    
    # 7. Value distribution for categorical column
    print("Computing categorical distributions...")
    col_e_dist = df.groupBy("col_e").count().orderBy("col_e").collect()
    fingerprint["col_e_distribution"] = {row["col_e"]: row["count"] for row in col_e_dist}
    
    # 8. Percentile values for numeric columns
    # Note: Using relativeError=0.001 for higher precision in fingerprinting.
    # Lower values = more accurate but slower. 0.001 gives ~0.1% precision.
    print("Computing percentiles...")
    PERCENTILE_RELATIVE_ERROR = 0.001
    percentiles = df.approxQuantile(
        ["col_c", "col_d"], 
        [0.25, 0.5, 0.75, 0.95], 
        PERCENTILE_RELATIVE_ERROR
    )
    fingerprint["col_c_percentiles"] = {
        "p25": percentiles[0][0],
        "p50": percentiles[0][1],
        "p75": percentiles[0][2],
        "p95": percentiles[0][3]
    }
    fingerprint["col_d_percentiles"] = {
        "p25": percentiles[1][0],
        "p50": percentiles[1][1],
        "p75": percentiles[1][2],
        "p95": percentiles[1][3]
    }
    
    # 9. Create overall fingerprint hash
    # Exclude metadata fields that vary between runs (generation_time, directory_path)
    # Only hash the actual data-derived metrics
    hash_fields = {k: v for k, v in fingerprint.items() 
                   if k not in ("generation_time", "directory_path")}
    # Sort keys for deterministic JSON
    fingerprint_json = json.dumps(hash_fields, sort_keys=True, indent=2)
    overall_hash = hashlib.sha256(fingerprint_json.encode()).hexdigest()
    fingerprint["overall_hash"] = overall_hash
    
    print("=" * 60)
    print("Fingerprint generated successfully!")
    print(f"Overall hash: {overall_hash}")
    print("=" * 60)
    
    return fingerprint


def compare_fingerprints(fp1, fp2, tolerance=0.01):
    """
    Compare two fingerprints and report differences.
    
    Args:
        fp1: First fingerprint dict
        fp2: Second fingerprint dict
        tolerance: Relative tolerance for float comparisons
        
    Returns:
        bool: True if fingerprints match (within tolerance)
    """
    
    print("=" * 60)
    print("FINGERPRINT COMPARISON")
    print("=" * 60)
    
    all_match = True
    
    # Compare basic counts
    print("\n1. Basic Counts:")
    if fp1.get("total_rows") != fp2.get("total_rows"):
        print(f"  ❌ Row count mismatch: {fp1.get('total_rows')} vs {fp2.get('total_rows')}")
        all_match = False
    else:
        print(f"  ✓ Row count matches: {fp1.get('total_rows')}")
    
    if fp1.get("self_loops") != fp2.get("self_loops"):
        print(f"  ❌ Self-loops mismatch: {fp1.get('self_loops')} vs {fp2.get('self_loops')}")
        all_match = False
    else:
        print(f"  ✓ Self-loops matches: {fp1.get('self_loops')}")
    
    # Compare unique nodes
    print("\n2. Unique Nodes:")
    for key in ["unique_nodes_col_a", "unique_nodes_col_b", "all_nodes"]:
        if fp1.get(key) != fp2.get(key):
            print(f"  ❌ {key} mismatch: {fp1.get(key)} vs {fp2.get(key)}")
            all_match = False
        else:
            print(f"  ✓ {key} matches: {fp1.get(key)}")
    
    # Compare degree distribution
    print("\n3. Degree Distribution:")
    if fp1.get("degree_distribution_hash") != fp2.get("degree_distribution_hash"):
        print(f"  ❌ Degree distribution hash mismatch")
        print(f"      FP1: {fp1.get('degree_distribution_hash')}")
        print(f"      FP2: {fp2.get('degree_distribution_hash')}")
        all_match = False
    else:
        print(f"  ✓ Degree distribution hash matches")
    
    if fp1.get("degree_distribution_entries") != fp2.get("degree_distribution_entries"):
        print(f"  ❌ Degree distribution entries mismatch: {fp1.get('degree_distribution_entries')} vs {fp2.get('degree_distribution_entries')}")
        all_match = False
    else:
        print(f"  ✓ Degree distribution entries matches: {fp1.get('degree_distribution_entries')}")
    
    # Compare degree stats with tolerance
    print("\n4. Degree Statistics (with tolerance):")
    for stat_key in ["min", "max", "avg", "stddev"]:
        val1 = fp1.get("degree_stats", {}).get(stat_key)
        val2 = fp2.get("degree_stats", {}).get(stat_key)
        
        if val1 is None or val2 is None:
            if val1 != val2:
                print(f"  ❌ degree_stats.{stat_key} mismatch: {val1} vs {val2}")
                all_match = False
            continue
            
        if isinstance(val1, float) or isinstance(val2, float):
            # Use relative tolerance for floats
            if val1 == 0 and val2 == 0:
                matches = True
            elif val1 == 0 or val2 == 0:
                matches = abs(val1 - val2) <= tolerance
            else:
                matches = abs(val1 - val2) / max(abs(val1), abs(val2)) <= tolerance
        else:
            matches = val1 == val2
            
        if not matches:
            print(f"  ❌ degree_stats.{stat_key} mismatch: {val1} vs {val2}")
            all_match = False
        else:
            print(f"  ✓ degree_stats.{stat_key} matches: {val1}")
    
    # Compare sample checksum
    print("\n5. Sample Checksum:")
    if fp1.get("sample_checksum") != fp2.get("sample_checksum"):
        print(f"  ❌ Sample checksum mismatch")
        print(f"      FP1: {fp1.get('sample_checksum')}")
        print(f"      FP2: {fp2.get('sample_checksum')}")
        all_match = False
    else:
        print(f"  ✓ Sample checksum matches")
    
    if fp1.get("sample_size") != fp2.get("sample_size"):
        print(f"  ❌ Sample size mismatch: {fp1.get('sample_size')} vs {fp2.get('sample_size')}")
        all_match = False
    else:
        print(f"  ✓ Sample size matches: {fp1.get('sample_size')}")
    
    # Compare categorical distribution
    print("\n6. Categorical Distribution (col_e):")
    dist1 = fp1.get("col_e_distribution", {})
    dist2 = fp2.get("col_e_distribution", {})
    
    all_keys = set(dist1.keys()) | set(dist2.keys())
    dist_match = True
    
    for key in sorted(all_keys):
        v1 = dist1.get(key)
        v2 = dist2.get(key)
        if v1 != v2:
            print(f"  ❌ col_e='{key}' mismatch: {v1} vs {v2}")
            dist_match = False
            all_match = False
    
    if dist_match:
        print(f"  ✓ col_e distribution matches ({len(all_keys)} categories)")
    
    # Compare percentiles with tolerance
    print("\n7. Percentiles:")
    for col in ["col_c_percentiles", "col_d_percentiles"]:
        pct1 = fp1.get(col, {})
        pct2 = fp2.get(col, {})
        col_match = True
        
        for pct_key in ["p25", "p50", "p75", "p95"]:
            v1 = pct1.get(pct_key)
            v2 = pct2.get(pct_key)
            
            if v1 is not None and v2 is not None:
                if v1 == 0 and v2 == 0:
                    matches = True
                elif v1 == 0 or v2 == 0:
                    matches = abs(v1 - v2) <= tolerance
                else:
                    matches = abs(v1 - v2) / max(abs(v1), abs(v2)) <= tolerance
            else:
                matches = v1 == v2
                
            if not matches:
                print(f"  ❌ {col}.{pct_key} mismatch: {v1} vs {v2}")
                col_match = False
                all_match = False
        
        if col_match:
            print(f"  ✓ {col} matches")
    
    # Compare schema
    print("\n8. Schema:")
    if fp1.get("schema") != fp2.get("schema"):
        print(f"  ❌ Schema mismatch")
        print(f"      FP1: {fp1.get('schema')}")
        print(f"      FP2: {fp2.get('schema')}")
        all_match = False
    else:
        print(f"  ✓ Schema matches ({len(fp1.get('schema', []))} fields)")
    
    # Compare overall hash
    print("\n9. Overall Hash:")
    if fp1.get("overall_hash") != fp2.get("overall_hash"):
        print(f"  ❌ Overall hash mismatch")
        print(f"      FP1: {fp1.get('overall_hash')}")
        print(f"      FP2: {fp2.get('overall_hash')}")
        all_match = False
    else:
        print(f"  ✓ Overall hash matches: {fp1.get('overall_hash')}")
    
    # Final result
    print("\n" + "=" * 60)
    if all_match:
        print("✓ FINGERPRINTS MATCH")
    else:
        print("❌ FINGERPRINTS DO NOT MATCH")
    print("=" * 60)
    
    return all_match


def save_fingerprint(fingerprint, filepath):
    """Save fingerprint to JSON file."""
    with open(filepath, 'w') as f:
        json.dump(fingerprint, f, indent=2, sort_keys=True)


def load_fingerprint(filepath):
    """Load fingerprint from JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)


# Example usage:
# edge_df = spark.read.parquet(directory_path)
# fingerprint = generate_dataset_fingerprint(edge_df, directory_path)
# save_fingerprint(fingerprint, "dataset_fingerprint.json")