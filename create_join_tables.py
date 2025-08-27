# -------------------------------------
# Step 1. Read all sheets from the Excel file.
# -------------------------------------
# excel_path = "datagen_schema.xlsx" # update this path as necessary
excel_path = "../HPE_NVDA_datagen.xlsx"

# Read every sheet into a dictionary: keys are sheet names, values are DataFrames.
sheets = pd.read_excel(excel_path, sheet_name=None)
# sheets = spark.read.  
sheet_names = list(sheets.keys())
print("Found sheets:", sheet_names)

# -------------------------------------
# Step 2. Process the tables overview (first sheet)
# -------------------------------------
# Assumption: The first sheet (e.g. "Tables") lists the table names and approximate row counts.
tables_overview_df = sheets[sheet_names[0]]
# Adjust these column names if your Excel file uses different names.
table_names = tables_overview_df["masked_table_id"].tolist()
approx_row_counts = tables_overview_df["num_rows_approx"].tolist()

print("Tables and approximate row counts:")
for tbl, cnt in zip(table_names, approx_row_counts):
    print(f"  {tbl}: ~{cnt} rows")

# -------------------------------------
# Step 3. Read each table's metadata (columns, types, etc.)
# -------------------------------------
# Here we assume that the sheet name for each table is the same as the table name.
table_metadata = {}
for tbl in table_names:
    if tbl in sheets:
        meta_df = sheets[tbl]
        table_metadata[tbl] = meta_df
        print(f"Loaded metadata for table '{tbl}'.")
    else:
        print(f"Warning: No metadata sheet found for table '{tbl}'.")

# -------------------------------------
# Step 4. Define a mapping from your Excel type names to Spark types.
# -------------------------------------
spark_type_mapping = {
    "StringType()": StringType(),
    "StringType": StringType(),
    "IntegerType()": IntegerType(),
    "IntegerType()": IntegerType(),
    "LongType()": LongType(),
    "FloatType()": FloatType(),
    "DoubleType()": DoubleType(),
    "BooleanType()": BooleanType(),
    "BooleanType()": BooleanType(),
    "DateType()": DateType(),
    "TimestampType()": TimestampType(),
    "ArrayType(IntegerType(), True)": ArrayType(IntegerType(), True),
    "ArrayType(StringType(), True)": ArrayType(StringType(), True)
}

def create_schema(meta_df):
    """
    Create a Spark schema (StructType) from the metadata DataFrame.
    For numerical types, if "min" and "max" are provided, they are stored in the field metadata.
    This version ensures that the type from the spreadsheet is used (if it matches).
    """
    fields = []
    # Ensure that the range columns exist in the DataFrame.
    has_range = ("min" in meta_df.columns) and ("max" in meta_df.columns)
    
    for idx, row in meta_df.iterrows():
        col_name = row["masked_column_name"]
        # Convert the Type from the spreadsheet to a lower-case string.
        type_str = str(row["spark_data_type"]).strip() if pd.notna(row["spark_data_type"]) else "string"
        spark_type = spark_type_mapping.get(type_str)
        
        if spark_type is None:
            # If the type is not recognized, warn and default to StringType.
            print(f"Warning: Unrecognized type '{row['spark_data_type']}' for column '{col_name}'. Using StringType.")
            spark_type = StringType()
        
        md = {}
        # For numerical types, if min and max values are provided, store them in metadata.
        if isinstance(spark_type, (IntegerType, LongType, FloatType, DoubleType)) and has_range:
            if pd.notna(row["min"]) and pd.notna(row["max"]):
                md["min"] = row["min"]
                md["max"] = row["max"]
        
        fields.append(StructField(col_name, spark_type, True, metadata=md))
    
    return StructType(fields)

# Create a dictionary of schemas for each table.
schemas = {}
for tbl, meta_df in table_metadata.items():
    schema = create_schema(meta_df)
    schemas[tbl] = schema
    print(f"Schema for table '{tbl}': {schema}")


# -------------------------------------
# Step 5. Process join information.
# -------------------------------------
# Assumption: The final sheet (last sheet) is named "Joins" and holds the join definitions.
join_info_df = sheets[sheet_names[1]]
joins = []
# Here we assume join_info_df has columns: "LeftTable", "LeftColumn", "RightTable", "RightColumn", and optionally "JoinType"
for idx, row in join_info_df.iterrows():
    join_detail = {
        "left_table": row["table1"],
        "right_table": row["table2"],
        "join_method": row["join_method"],
        "left_column": row["column1"],
        "right_column": row["column2"]
    }
    joins.append(join_detail)

# ========================================
# PART 2: Generate random data for each table and register as temp views
# ========================================

def generate_random_dataframe(schema, num_rows):
    """
    Simpler version with basic array generation
    """
    df = spark.range(num_rows)
    
    for field in schema.fields:
        col_name = field.name
        dt = field.dataType
        md = field.metadata or {}
        
        if isinstance(dt, (IntegerType, LongType)):
            min_val = md.get("min", 1)
            max_val = md.get("max", 1000)
            expr = (F.rand() * (float(max_val) - float(min_val)) + float(min_val))
            if isinstance(dt, IntegerType):
                df = df.withColumn(col_name, expr.cast("int"))
            else:
                df = df.withColumn(col_name, expr.cast("long"))
                
        elif isinstance(dt, (FloatType, DoubleType)):
            min_val = md.get("min", 0.0)
            max_val = md.get("max", 1000.0)
            expr = (F.rand() * (float(max_val) - float(min_val)) + float(min_val))
            if isinstance(dt, FloatType):
                df = df.withColumn(col_name, expr.cast("float"))
            else:
                df = df.withColumn(col_name, expr.cast("double"))
                
        elif isinstance(dt, BooleanType):
            df = df.withColumn(col_name, F.rand() > 0.5)
            
        elif isinstance(dt, DateType):
            df = df.withColumn(col_name, F.expr("date_add('2000-01-01', cast(rand() * 9000 as int))"))
            
        elif isinstance(dt, TimestampType):
            df = df.withColumn(col_name, F.expr("to_timestamp(date_add('2000-01-01', cast(rand() * 9000 as int)))"))
            
        elif isinstance(dt, StringType):
            df = df.withColumn(col_name, 
                              F.concat(F.lit("str_"), 
                                     F.abs(F.hash(F.col("id"), F.rand())).cast("string")))
            
        elif isinstance(dt, ArrayType):
            # Simpler array generation - fixed size arrays
            element_type = dt.elementType
            
            if isinstance(element_type, IntegerType):
                # Create array of 3 random integers
                df = df.withColumn(col_name, 
                    F.array(
                        (F.rand() * 100).cast("int"),
                        (F.rand() * 100).cast("int"), 
                        (F.rand() * 100).cast("int")
                    ))
                
            elif isinstance(element_type, LongType):
                # Create array of 3 random longs
                df = df.withColumn(col_name,
                    F.array(
                        (F.rand() * 1000).cast("long"),
                        (F.rand() * 1000).cast("long"),
                        (F.rand() * 1000).cast("long")
                    ))
                
            elif isinstance(element_type, StringType):
                # Create array of 3 random strings
                df = df.withColumn(col_name,
                    F.array(
                        F.concat(F.lit("item_"), (F.rand() * 100).cast("int").cast("string")),
                        F.concat(F.lit("item_"), (F.rand() * 100).cast("int").cast("string")),
                        F.concat(F.lit("item_"), (F.rand() * 100).cast("int").cast("string"))
                    ))
                
            else:
                # Default to empty array for unsupported types
                df = df.withColumn(col_name, F.array())

        else:
            df = df.withColumn(col_name, F.lit(None))
            
    return df.drop("id")

# Create and register a DataFrame for each table using the distributed random data generation.
# NOTE: THIS WAS SCALED DOWN FOR TESTING PURPOSES. UNCOMMENT LINE 74 AND COMMENT OUT LINES 68-73 FOR REAL TESTING
dfs = {}
for tbl, count in zip(table_names, approx_row_counts):
    if tbl != 'table_a':
        schema = schemas[tbl]
        if tbl == 'table_c':
            num_rows = 21000000
        else:
            num_rows = int(count)
        # num_rows = int(count)
        df = generate_random_dataframe(schema, num_rows)
        dfs[tbl] = df
        print(f"Created DataFrame for table '{tbl}' with {num_rows} random rows.")

table_b = dfs['table_b']
table_c = dfs['table_c']
table_d = dfs['table_d']
table_e = dfs['table_e']


# =========================
# CONFIGURATION - Set your desired match percentages here
# =========================
MATCH_PERCENTAGE_A = 0.001  # 0.1% of table_a rows will match
MATCH_PERCENTAGE_C = 0.01  # 1% of table_c rows will match
MATCH_PERCENTAGE_D = 0.01  # 1% of table_d rows will match
MATCH_PERCENTAGE_E = 0.01  # 1% of table_e rows will match

print("=" * 60)
print("FORCING TABLES TO MATCH TABLE_B VALUES")
print(f"Match percentages: A={MATCH_PERCENTAGE_A*100}%, C={MATCH_PERCENTAGE_C*100}%, D={MATCH_PERCENTAGE_D*100}%, E={MATCH_PERCENTAGE_E*100}%")
print("=" * 60)

# =========================
# Force table_a (4 columns: col_a, col_c, col_b, col_d)
# =========================
print("\n1. Processing table_a...")
table_a_combos_list = (
    table_b
    .select("col_b_8", "col_b_3", "col_b_9", "col_b_1")
    .distinct()
    .filter(
        F.col("col_b_8").isNotNull() & 
        F.col("col_b_3").isNotNull() & 
        F.col("col_b_9").isNotNull() & 
        F.col("col_b_1").isNotNull()
    )
    .collect()
)

combo_count_a = len(table_a_combos_list)
print(f"   Distinct combinations for table_a: {combo_count_a}")

combos_a_with_id = [
    Row(
        new_col_a=combo['col_b_8'],
        new_col_c=combo['col_b_3'],
        new_col_b=combo['col_b_9'],
        new_col_d=combo['col_b_1'],
        combo_id=idx + 1
    )
    for idx, combo in enumerate(table_a_combos_list)
]

table_a_combos_df = spark.createDataFrame(combos_a_with_id)

# Add a random number to each row to decide if it should be forced
table_a_forced = (
    table_a
    .withColumn("should_force", F.rand() < MATCH_PERCENTAGE_A)
    .withColumn("combo_id", 
        F.when(F.col("should_force"), F.floor(F.rand() * combo_count_a) + 1)
        .otherwise(F.lit(None))
    )
    # Left join to preserve all rows
    .join(
        F.broadcast(table_a_combos_df),
        "combo_id",
        "left"
    )
    # For forced rows, use new values; for others, keep original
    .withColumn("col_a", F.coalesce("new_col_a", "col_a"))
    .withColumn("col_c", F.coalesce("new_col_c", "col_c"))
    .withColumn("col_b", F.coalesce("new_col_b", "col_b"))
    .withColumn("col_d", F.coalesce("new_col_d", "col_d"))
    .drop("should_force", "combo_id", "new_col_a", "new_col_c", "new_col_b", "new_col_d")
    .select(*table_a.columns)
)

table_a_forced.write.mode("overwrite").parquet("/mnt/weka/temp_forced_table_a")
table_a_forced = spark.read.parquet("/mnt/weka/temp_forced_table_a")
print(f"   ✓ table_a_forced created ({MATCH_PERCENTAGE_A*100}% forced)")

# =========================
# Force table_c (3 columns: col_c_10, col_c_9, col_c_11)
# =========================
print("\n2. Processing table_c...")
table_c_combos_list = (
    table_b
    .select("col_b_8", "col_b_9", "col_b_3")
    .distinct()
    .filter(
        F.col("col_b_8").isNotNull() & 
        F.col("col_b_9").isNotNull() & 
        F.col("col_b_3").isNotNull()
    )
    .collect()
)

combo_count_c = len(table_c_combos_list)
print(f"   Distinct combinations for table_c: {combo_count_c}")

combos_c_with_id = [
    Row(
        new_col_c_10=combo['col_b_8'],
        new_col_c_9=combo['col_b_9'],
        new_col_c_11=combo['col_b_3'],
        combo_id=idx + 1
    )
    for idx, combo in enumerate(table_c_combos_list)
]

table_c_combos_df = spark.createDataFrame(combos_c_with_id)

table_c_forced = (
    table_c
    .withColumn("should_force", F.rand() < MATCH_PERCENTAGE_C)
    .withColumn("combo_id", 
        F.when(F.col("should_force"), F.floor(F.rand() * combo_count_c) + 1)
        .otherwise(F.lit(None))
    )
    .join(
        F.broadcast(table_c_combos_df),
        "combo_id",
        "left"
    )
    .withColumn("col_c_10", F.coalesce("new_col_c_10", "col_c_10"))
    .withColumn("col_c_9", F.coalesce("new_col_c_9", "col_c_9"))
    .withColumn("col_c_11", F.coalesce("new_col_c_11", "col_c_11"))
    .drop("should_force", "combo_id", "new_col_c_10", "new_col_c_9", "new_col_c_11")
    .select(*table_c.columns)
)

table_c_forced.write.mode("overwrite").parquet("/mnt/weka/temp_forced_table_c")
table_c_forced = spark.read.parquet("/mnt/weka/temp_forced_table_c")
print(f"   ✓ table_c_forced created ({MATCH_PERCENTAGE_C*100}% forced)")

# =========================
# Force table_d (2 columns: col_d_0, col_d_1)
# =========================
print("\n3. Processing table_d...")
table_d_combos_list = (
    table_b
    .select("col_b_8", "col_b_9")
    .distinct()
    .filter(
        F.col("col_b_8").isNotNull() & 
        F.col("col_b_9").isNotNull()
    )
    .collect()
)

combo_count_d = len(table_d_combos_list)
print(f"   Distinct combinations for table_d: {combo_count_d}")

combos_d_with_id = [
    Row(
        new_col_d_0=combo['col_b_8'],
        new_col_d_1=combo['col_b_9'],
        combo_id=idx + 1
    )
    for idx, combo in enumerate(table_d_combos_list)
]

table_d_combos_df = spark.createDataFrame(combos_d_with_id)

table_d_forced = (
    table_d
    .withColumn("should_force", F.rand() < MATCH_PERCENTAGE_D)
    .withColumn("combo_id", 
        F.when(F.col("should_force"), F.floor(F.rand() * combo_count_d) + 1)
        .otherwise(F.lit(None))
    )
    .join(
        F.broadcast(table_d_combos_df),
        "combo_id",
        "left"
    )
    .withColumn("col_d_0", F.coalesce("new_col_d_0", "col_d_0"))
    .withColumn("col_d_1", F.coalesce("new_col_d_1", "col_d_1"))
    .drop("should_force", "combo_id", "new_col_d_0", "new_col_d_1")
    .select(*table_d.columns)
)

table_d_forced.write.mode("overwrite").parquet("/mnt/weka/temp_forced_table_d")
table_d_forced = spark.read.parquet("/mnt/weka/temp_forced_table_d")
print(f"   ✓ table_d_forced created ({MATCH_PERCENTAGE_D*100}% forced)")

# =========================
# Force table_e (1 column: col_e_0)
# =========================
print("\n4. Processing table_e...")
table_e_values_list = (
    table_b
    .select("col_b_8")
    .distinct()
    .filter(F.col("col_b_8").isNotNull())
    .collect()
)

value_count_e = len(table_e_values_list)
print(f"   Distinct values for table_e: {value_count_e}")

values_e_with_id = [
    Row(
        new_col_e_0=value['col_b_8'],
        value_id=idx + 1
    )
    for idx, value in enumerate(table_e_values_list)
]

table_e_values_df = spark.createDataFrame(values_e_with_id)

table_e_forced = (
    table_e
    .withColumn("should_force", F.rand() < MATCH_PERCENTAGE_E)
    .withColumn("value_id", 
        F.when(F.col("should_force"), F.floor(F.rand() * value_count_e) + 1)
        .otherwise(F.lit(None))
    )
    .join(
        F.broadcast(table_e_values_df),
        "value_id",
        "left"
    )
    .withColumn("col_e_0", F.coalesce("new_col_e_0", "col_e_0"))
    .drop("should_force", "value_id", "new_col_e_0")
    .select(*table_e.columns)
)

table_e_forced.write.mode("overwrite").parquet("/mnt/weka/temp_forced_table_e")
table_e_forced = spark.read.parquet("/mnt/weka/temp_forced_table_e")
print(f"   ✓ table_e_forced created ({MATCH_PERCENTAGE_E*100}% forced)")

table_b.write.mode("overwrite").parquet("/mnt/weka/temp_table_b")
table_b = spark.read.parquet("/mnt/weka/temp_table_b")