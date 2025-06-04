# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`default`.`motor_vehicle_insurance_data` LIMIT 10;

# COMMAND ----------

!pip install faker tqdm

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType # Import more types as needed
# Faker and tqdm should have been installed in Step 1 via %pip install
from faker import Faker
from tqdm import tqdm # tqdm is mainly useful for loops running on the driver node

# --- 1. Configuration ---
input_table_name = "workspace.default.motor_vehicle_insurance_data"
output_table_name = "workspace.default.motor_insurance_with_consistent_synthetic_pii_seeded"

locale_setting = 'en_US'
sample_rows_to_show = 20
random_seed = 42
# --- End of Configuration ---

print(f"Reading data from Databricks table: {input_table_name} ...")
try:
    # --- 2. Read data from the Databricks table ---
    df_spark = spark.table(input_table_name)

    print(f"Data read successfully from table. Total rows: {df_spark.count()}")
    print("Input DataFrame schema:")
    df_spark.printSchema()

except Exception as e:
    print(f"Error reading from table {input_table_name}: {e}")
    raise e # This will stop cell execution and display the error

# --- 3. Core Logic: Generate and Map PII ---
# Ensure 'ID' is the correct name of your unique identifier column in the table.

# 3.1. Check for 'ID' column and get unique ID values
if "ID" not in df_spark.columns:
    error_message = f"ERROR: Column 'ID' not found in the table {input_table_name}. Please check the column name for unique identifiers."
    print(error_message)
    # dbutils.notebook.exit(error_message) # Use this to stop the notebook with a message
    raise ValueError(error_message) # Or raise an error

unique_ids_df = df_spark.select("ID").distinct()
print(f"Found {unique_ids_df.count()} unique IDs in the 'ID' column.")

# 3.2. Generate PII mapping for each unique ID (done on the driver node)
print("Generating synthetic PII mapping for each unique ID on the driver node...")
# Convert to Pandas Series to better handle various ID data types and get unique values efficiently on the driver
unique_ids_pandas_series = unique_ids_df.toPandas()['ID']
unique_ids_list = sorted(list(unique_ids_pandas_series.unique()))

fake = Faker(locale_setting)
Faker.seed(random_seed) # Set seed for Faker to ensure reproducibility
print(f"Faker seed set to: {random_seed}")

pii_records = []
for uid in tqdm(unique_ids_list, desc="Generating unique PII mappings"):
    pii_records.append((
        uid, # ID
        fake.first_name(),
        fake.last_name(),
        fake.zipcode(),
        fake.license_plate(),
        fake.ssn()
    ))

# 3.3. Create a Spark DataFrame for the PII map
# Dynamically get the data type of the original ID column
id_data_type = df_spark.schema["ID"].dataType

pii_schema = StructType([
    StructField("ID_map", id_data_type, False), # Match the original ID's data type
    StructField("First_Name_map", StringType(), False),
    StructField("Last_Name_map", StringType(), False),
    StructField("ZipCode_map", StringType(), False),
    StructField("LicensePlateNumber_map", StringType(), False),
    StructField("SSN_map", StringType(), False)
])

pii_map_df = spark.createDataFrame(pii_records, schema=pii_schema)
print("Unique PII mappings DataFrame created.")

# 3.4. Join the PII map back to the original DataFrame
print("Mapping PII to the entire DataFrame using a join...")
df_processed = df_spark.join(
    pii_map_df,
    df_spark["ID"] == pii_map_df["ID_map"], # Join condition
    "left_outer"
).select(
    df_spark["*"], # Select all columns from the original DataFrame
    col("First_Name_map").alias("First_Name"),
    col("Last_Name_map").alias("Last_Name"),
    col("ZipCode_map").alias("ZipCode"),
    col("LicensePlateNumber_map").alias("LicensePlateNumber"),
    col("SSN_map").alias("SSN")
)

print("PII mapping completed.")

# --- 4. Save and Display Results ---
print(f"Saving processed data to Databricks table: {output_table_name} ...")
try:
    # Save the processed DataFrame as a new Databricks table
    df_processed.write.mode("overwrite").saveAsTable(output_table_name)
    print(f"Data successfully saved to table '{output_table_name}'.")
except Exception as e:
    print(f"Error saving data to table {output_table_name}: {e}")
    raise e

print("\n" + "="*50)
print(f"Processing completed. Here are up to {sample_rows_to_show} rows of the final result from the new table:")
print("="*50)

# Display a sample of the data from the newly created table
display(spark.table(output_table_name).limit(sample_rows_to_show))

print(f"\nProcessing workflow completed. Output is in table: {output_table_name}")

# COMMAND ----------

#!/usr/bin/env python3
"""
Motor Insurance Dataset Quality Improvement Script (Native Spark for Databricks)
Addresses data quality issues, using UDFs for robust casting for older Spark versions.
PII MASKING HAS BEEN REMOVED from this version.
"""

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, TimestampType, DateType, IntegerType, DoubleType, LongType, BooleanType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import logging
import pandas as pd # For pd.isna in UDF
import hashlib # Kept import in case other non-PII hashing is ever needed, but not used for PII now

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Helper UDFs ---

# UDF for Date Standardization
def standardize_date_logic(date_val):
    if date_val is None: return None
    if isinstance(date_val, (datetime, pd.Timestamp)):
        if pd.isna(date_val): return None
        return date_val.strftime('%Y-%m-%d')
    date_str = str(date_val)
    if date_str.lower() in ['', 'nan', 'nat', 'none', 'null']: return None
    formats = [
        '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d',
        '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y', '%m-%d-%Y',
        '%d.%m.%Y', '%Y.%m.%d', '%b %d %Y', '%d %b %Y', '%B %d %Y', '%d %B %Y'
    ]
    for fmt in formats:
        try: return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except (ValueError, TypeError): continue
    logger.debug(f"Could not parse date string with UDF: {date_str}")
    return None # Or return original string if parsing fails
spark_standardize_date_udf = F.udf(standardize_date_logic, StringType())

# UDF for Hashing (Definition remains, but will not be used for LicensePlateNumber masking)
salt_generic = "generic_salt_for_other_uses_2025" 
def hash_value_udf_logic(value, salt):
    if value is None: return None
    return hashlib.sha256((str(value) + salt).encode()).hexdigest()[:16]
# generic_hash_udf = F.udf(lambda val: hash_value_udf_logic(val, salt_generic), StringType())
# Note: hash_license_plate_udf is effectively removed by not calling it.

# --- UDFs for Safe Casting (Alternative to try_cast) ---
def safe_cast_to_int_py(value_str):
    if value_str is None: return None
    try: return int(value_str)
    except (ValueError, TypeError): return None
safe_cast_int_udf = F.udf(safe_cast_to_int_py, IntegerType())

def safe_cast_to_long_py(value_str): # For BIGINT
    if value_str is None: return None
    try: return int(value_str) # Python's int handles large numbers, Spark maps to LongType
    except (ValueError, TypeError): return None
safe_cast_long_udf = F.udf(safe_cast_to_long_py, LongType())

def safe_cast_to_double_py(value_str):
    if value_str is None: return None
    try: return float(value_str)
    except (ValueError, TypeError): return None
safe_cast_double_udf = F.udf(safe_cast_to_double_py, DoubleType())


# --- Spark Data Quality Fixer Class ---
class SparkDataQualityFixer:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(self.__class__.__name__)

    def fix_inconsistent_date_formats(self, df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        self.logger.info("Standardizing date formats using Spark UDF...")
        date_cols = [f.name for f in df.schema.fields if 'Date' in f.name or 'DATE' in f.name or 'date' in f.name or isinstance(f.dataType, (DateType, TimestampType))]
        if not date_cols:
            self.logger.info("No date columns found for standardization.")
            return df
        self.logger.info(f"Potential date columns for standardization: {date_cols}")
        for col_name in date_cols:
            self.logger.info(f"Standardizing date column: {col_name}")
            df = df.withColumn(col_name, spark_standardize_date_udf(F.col(col_name)))
        return df

    def fix_incorrect_values(self, df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        self.logger.info("Fixing incorrect values using Spark operations (with safe cast UDFs)...")
        
        numeric_cols_config = {
            'Premium':           {'use_abs': True,  'type': DoubleType()},
            'Cost_claims_year':  {'use_abs': True,  'type': DoubleType()},
            'Value_vehicle':     {'use_abs': True,  'type': DoubleType()},
            'Power':             {'use_abs': False, 'type': DoubleType()},
            'Cylinder_capacity': {'use_abs': False, 'type': IntegerType()},
            'Weight':            {'use_abs': False, 'type': DoubleType()},
            'Length':            {'use_abs': False, 'type': DoubleType()},
            'Seniority':         {'use_abs': False, 'type': IntegerType()},
            'Policies_in_force': {'use_abs': False, 'type': LongType()},
            'Max_policies':      {'use_abs': False, 'type': LongType()},
            'Max_products':      {'use_abs': False, 'type': LongType()},
            'N_claims_year':     {'use_abs': False, 'type': LongType()},
            'N_claims_history':  {'use_abs': False, 'type': LongType()}
        }

        for col_name, config in numeric_cols_config.items():
            if col_name in df.columns:
                self.logger.info(f"Safely casting and processing numeric column: {col_name} to target type {config['type']}")
                col_as_string = F.col(col_name).cast(StringType())
                
                casted_col = None
                if isinstance(config['type'], IntegerType):
                    casted_col = safe_cast_int_udf(col_as_string)
                elif isinstance(config['type'], LongType):
                    casted_col = safe_cast_long_udf(col_as_string)
                elif isinstance(config['type'], DoubleType):
                    casted_col = safe_cast_double_udf(col_as_string)
                else:
                    self.logger.warning(f"Unsupported target type {config['type']} for UDF casting on column {col_name}. Keeping original.")
                    df = df.withColumn(col_name, F.col(col_name)) 
                    continue

                df = df.withColumn(col_name, casted_col)
                
                if config['use_abs']:
                    df = df.withColumn(col_name, F.when(F.col(col_name) < 0, F.abs(F.col(col_name))).otherwise(F.col(col_name)))
                else:
                    df = df.withColumn(col_name, F.when(F.col(col_name) < 0, F.lit(None).cast(config['type'])).otherwise(F.col(col_name)))
            else:
                self.logger.info(f"Numeric column {col_name} not found in DataFrame. Skipping.")
        
        if 'Year_matriculation' in df.columns:
            self.logger.info("Correcting out-of-range 'Year_matriculation' values...")
            df = df.withColumn('Year_matriculation', safe_cast_int_udf(F.col('Year_matriculation').cast(StringType())))
            current_year = datetime.now().year
            min_year = 1900
            df = df.withColumn('Year_matriculation',
                F.when((F.col('Year_matriculation') > current_year) | (F.col('Year_matriculation') < min_year) | F.col('Year_matriculation').isNull(), F.lit(None).cast(IntegerType()))
                 .otherwise(F.col('Year_matriculation')))

        if 'Date_birth' in df.columns:
            self.logger.info("Correcting invalid 'Date_birth' values (age constraints)...")
            min_age_years, max_age_years = 16, 110
            df = df.withColumn("Date_birth_dt", F.to_date(F.col("Date_birth"), "yyyy-MM-dd"))
            min_valid_bdate_expr = F.expr(f"date_sub(current_date(), {max_age_years * 365 + int(max_age_years/4)})")
            max_valid_bdate_expr = F.expr(f"date_sub(current_date(), {min_age_years * 365 + int(min_age_years/4)})")
            df = df.withColumn('Date_birth',
                F.when(F.col("Date_birth_dt").isNull() | (F.col('Date_birth_dt') < min_valid_bdate_expr) | (F.col('Date_birth_dt') > max_valid_bdate_expr), F.lit(None).cast(StringType()))
                 .otherwise(F.col('Date_birth')))
            df = df.drop("Date_birth_dt")
        return df

    def fix_zero_placeholders(self, df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        self.logger.info("Fixing zero placeholders and empty strings...")
        zero_as_null_cols_config = { 
            'Weight': DoubleType(), 'Length': DoubleType(), 'Seniority': IntegerType()
        }
        for col_name, num_type in zero_as_null_cols_config.items():
            if col_name in df.columns:
                self.logger.info(f"In {col_name}, replacing numeric 0 with NULL if it's a placeholder.")
                zero_lit = F.lit(0).cast(num_type)
                df = df.withColumn(col_name, F.when(F.col(col_name) == zero_lit, F.lit(None).cast(df.schema[col_name].dataType)).otherwise(F.col(col_name)))
        
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col_name in string_cols:
            df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == '', F.lit(None).cast(StringType())).otherwise(F.col(col_name)))
        return df

    def add_timestamping(self, df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        self.logger.info("Adding timestamping information...")
        run_ts = F.current_timestamp()
        df = df.withColumn('Processing_Timestamp', F.date_format(run_ts, 'yyyy-MM-dd HH:mm:ss'))
        actual_proc_date = datetime.now()
        df = df.withColumn('Data_Version', F.lit(f"{actual_proc_date.strftime('%Y-%m-%d')}-v1-DQ-Spark-NoMask")) # Updated version string
        if 'Last_Modified' not in df.columns:
            self.logger.info("Adding 'Last_Modified' based on 'Date_last_renewal' or Processing_Timestamp.")
            df = df.withColumn('Last_Modified', F.coalesce(F.col('Date_last_renewal'), F.date_format(run_ts, 'yyyy-MM-dd HH:mm:ss')))
        return df

    def mask_pii_fields(self, df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        self.logger.info("PII masking step: No PII fields will be masked in this version of the script.")
        # Previously, SSN was masked and LicensePlateNumber was hashed.
        # This method now does nothing to those fields, leaving them as they are.
        # If specific columns need to be *dropped* for privacy, that logic would go here.
        # For example, if you wanted to drop SSN entirely:
        # if 'SSN' in df.columns:
        #     self.logger.info("Dropping 'SSN' column as per no-masking policy (example).")
        #     df = df.drop('SSN')
        return df # Return the DataFrame unchanged regarding PII masking

    def run_all_fixes(self, input_table_name: str, output_table_name: str):
        self.logger.info(f"Starting Spark Data Quality Fixes for input: {input_table_name} (NO PII MASKING)")
        try:
            df = self.spark.table(input_table_name)
            self.logger.info("Input DataFrame schema:"); df.printSchema()
            self.logger.info(f"Initial record count: {df.count()}")

            df = self.fix_inconsistent_date_formats(df)
            df = self.fix_incorrect_values(df)
            df = self.fix_zero_placeholders(df)
            df = self.add_timestamping(df)
            df = self.mask_pii_fields(df) # This will now log that no masking is done

            self.logger.info("Final DataFrame schema before writing:"); df.printSchema()
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table_name)
            self.logger.info(f"Cleaned data saved to table: {output_table_name}")
            self.logger.info(f"Final record count: {self.spark.table(output_table_name).count()}")
        except Exception as e:
            self.logger.error(f"Error during data quality process: {e}", exc_info=True)
            raise

def main(spark_session: SparkSession):
    logger.info("Starting Data Quality Script (Native Spark, Safe Cast UDFs, NO PII MASKING)...")
    input_tbl = "workspace.default.motor_insurance_with_consistent_synthetic_pii_seeded"
    output_tbl = "workspace.default.motor_insurance_high_quality"
    fixer = SparkDataQualityFixer(spark_session)
    fixer.run_all_fixes(input_tbl, output_tbl)
    logger.info(f"Script finished. Output in table: {output_tbl}")
    logger.info("Displaying sample from output table:")
    spark_session.table(output_tbl).show(10, truncate=False)

if __name__ == "__main__":
    spark_session_glob = spark if 'spark' in locals() else SparkSession.builder.appName("SparkDQAppNoMask").enableHiveSupport().getOrCreate()
    main(spark_session_glob)