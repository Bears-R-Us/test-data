# Test Data Generation and Spark Performance Testing Framework

A comprehensive framework for generating large-scale synthetic datasets and testing Apache Spark performance, with support for both standard Spark and NVIDIA RAPIDS acceleration. This repository provides tools for creating realistic test data with controlled characteristics, including graph-based datasets with specific degree distributions and complex join scenarios.

## Repository Structure

#### `datagen_schema.xlsx`
Excel-based schema definition file that drives the entire data generation process.
- **Table Definitions**: Contains metadata for all tables including table names (table_a through table_e) and approximate row counts (from 14K to 100B rows)
- **Column Specifications**: Defines column names, Spark data types (StringType, LongType, IntegerType, ArrayType), and value ranges (min/max) for numeric columns
- **Join Relationships**: Specifies join conditions between tables with column mappings (e.g., table_a.col_a joins with table_b.col_b_8, table_c.col_c_10, etc.)

#### `Data Generation and Testing Notebook.ipynb`
Original Notebook for Generating Synthetic Data and Benchmarking Performance in Spark and Spark-Rapids
- **Synthetic Graph Generation**: Creates network graphs with configurable degree distributions using NetworkX, supporting power-law and custom distributions for realistic social network or web-scale graph simulation. Originally based on the [Barabasi-Albert Model](https://en.wikipedia.org/wiki/Barab%C3%A1si%E2%80%93Albert_model).
- **Data Scaling Pipeline**: Implements an iterative approach to scale datasets to target sizes (e.g., >1TB) by doubling data with controlled noise injection while preserving statistical properties
- **Non-Graph Table Generation**: Produces randomized DataFrames matching specific schemas from Excel specifications, supporting all Spark data types including arrays and complex types
- **Comprehensive Testing Suite**: Includes benchmarks for sorts, group-by operations, multi-table joins, breadth-first search, and PageRank algorithms

#### Alternative: `create_join_tables.py`
Specialized script for creating tables with guaranteed join matches for testing complex workflows.
- **Schema-Driven Generation**: Reads table schemas and join relationships from Excel files to ensure data consistency across related tables
- **Forced Join Matching**: Implements a sophisticated mechanism to force a configurable percentage (0.1-1%) of rows to match join conditions, simulating realistic data skew
- **Broadcast Optimization**: Uses broadcast joins for dimension tables (table_b with 14k rows) to optimize join performance
- **Column Mapping**: Automatically maps columns between tables based on join specifications (e.g., table_a.col_a → table_b.col_b_8)
- **Persistent Storage**: Writes forced tables to Parquet format in `/mnt/weka/` for reuse across multiple test runs

#### OLD: `Synthetic Degree Distribution.ipynb`
Standalone notebook for creating and validating graph degree distributions. Has since been integrated into `Data Generation and Testing Notebook.ipynb`
- **Power-Law Distribution**: Generates synthetic degree distributions following power-law decay with configurable slope (-2 default) and maximum probability (0.5 default)
- **Configuration Model**: Uses NetworkX's configuration model to create graphs matching the specified degree distribution exactly
- **Validation Metrics**: Compares empirical vs target distributions and reports degree statistics including average and maximum degrees
- **Node ID Generation**: Creates realistic node identifiers in the range 1M-10B to simulate large-scale networks
- **Feature Randomization**: Adds random features (col_c, col_d, col_e) to edges for testing aggregation operations

#### `Consolidated Forced Join.ipynb`
Join testing with forced data matching to ensure non-NULL values.
- **Multi-Table Join Pipeline**: Executes linked __inner__ joins (table_a → b → c → d → e) with ~5B rows in the primary edge_df table
- **Join Strategy Testing**: Tests broadcast joins for small tables and shuffle joins for large tables with configurable join methods
- **Execution Plan Analysis**: Includes detailed physical plans showing GpuShuffledSymmetricHashJoin operations and data volumes at each stage

## Key Features

### Data Generation Capabilities
- Generate datasets from MB to TB scale with precise size control
- Support for power-law, uniform, and custom degree distributions
- Full Spark SQL type support including arrays and complex nested structures
- Option for forced join relationships with configurable match percentages

### Performance Testing
- **Operations**: Sort, GroupBy, Join, Graph algorithms (BFS, PageRank)
- **Metrics**: Wall-clock time, shuffle metrics, task skew, throughput analysis
- **GPU Support**: NVIDIA RAPIDS configuration for GPU-accelerated processing

## Extra

### `spark_sql_dag_analyzer.py`
Attempt at automated Spark UI metrics extraction and analysis tool for deep performance insights.
- **Execution Plan Analysis**: Parses Spark SQL DAG to identify shuffles, joins, and compute operations with detailed timing breakdowns
- **System Performance Metrics**: Calculates shuffle throughput (GiB/s), compute vs I/O ratios, and network efficiency to identify hardware bottlenecks
- **Task-Level Skew Detection**: Analyzes min/median/max task times to identify data skew with ratios and specific stage/task IDs of outliers
- **Bottleneck Classification**: Uses evidence-based scoring to determine if performance issues are system/hardware or code/data distribution related
- **Comprehensive Reporting**: Generates detailed reports with slowest tasks, stage performance summaries, and actionable optimization recommendations
