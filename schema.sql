-- Ensure you are in the correct catalog and schema context.
-- Example:
-- USE CATALOG workspace;
-- USE SCHEMA default;

--------------------------------------------------------------------------------
-- Dimension Table: dim_customer
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.default.dim_customer (
    customer_id INTEGER NOT NULL,
    First_Name STRING,
    Last_Name STRING,
    Date_birth DATE,
    Date_driving_licence DATE,
    ZipCode STRING,
    Area STRING,
    SSN STRING,
    dw_created_timestamp TIMESTAMP,
    dw_updated_timestamp TIMESTAMP,
    dw_data_version STRING
)
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

INSERT INTO workspace.default.dim_customer
SELECT DISTINCT
    ID AS customer_id,
    First_Name,
    Last_Name,
    Date_birth,
    Date_driving_licence,
    CAST(ZipCode AS STRING) AS ZipCode,
    Area,
    SSN,
    current_timestamp() AS dw_created_timestamp,
    current_timestamp() AS dw_updated_timestamp,
    Data_Version AS dw_data_version
FROM
    workspace.default.motor_insurance_high_quality
WHERE
    ID IS NOT NULL;

--------------------------------------------------------------------------------
-- Dimension Table: dim_vehicle
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.default.dim_vehicle (
    vehicle_id STRING NOT NULL,
    Value_vehicle DECIMAL(10,2),
    Type_fuel STRING,
    Year_matriculation INTEGER,
    Power SMALLINT,
    Cylinder_capacity INTEGER,
    Weight INTEGER,
    Length DECIMAL(4,2),
    N_doors SMALLINT,
    dw_created_timestamp TIMESTAMP,
    dw_updated_timestamp TIMESTAMP,
    dw_data_version STRING
)
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

INSERT INTO workspace.default.dim_vehicle
SELECT DISTINCT
    LicensePlateNumber AS vehicle_id,
    Value_vehicle,
    Type_fuel,
    Year_matriculation,
    Power,
    Cylinder_capacity,
    Weight,
    Length,
    N_doors,
    current_timestamp() AS dw_created_timestamp,
    current_timestamp() AS dw_updated_timestamp,
    Data_Version AS dw_data_version
FROM
    workspace.default.motor_insurance_high_quality
WHERE
    LicensePlateNumber IS NOT NULL;

--------------------------------------------------------------------------------
-- Fact Table: fact_policy_activity
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.default.fact_policy_activity (
    customer_id INTEGER NOT NULL,
    vehicle_id STRING NOT NULL,
    Date_start_contract DATE NOT NULL,
    Date_last_renewal DATE,
    Date_next_renewal DATE,
    Distribution_channel SMALLINT,
    Seniority SMALLINT,
    Policies_in_force SMALLINT,
    Max_policies SMALLINT,
    Max_products SMALLINT,
    Lapse SMALLINT,
    Date_lapse DATE,
    Payment SMALLINT,
    Type_risk SMALLINT,
    Second_driver SMALLINT,
    Premium DECIMAL(10,2),
    Cost_claims_current_year DECIMAL(10,2),
    N_claims_current_year SMALLINT,
    N_claims_total_history SMALLINT,
    R_Claims_total_history DECIMAL(10,2),
    source_data_version STRING,
    source_processing_timestamp TIMESTAMP,
    source_last_modified DATE,
    dw_fact_load_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (Date_start_contract) 
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

INSERT INTO workspace.default.fact_policy_activity
SELECT
    ID AS customer_id,
    LicensePlateNumber AS vehicle_id,
    Date_start_contract,
    Date_last_renewal,
    Date_next_renewal,
    Distribution_channel,
    Seniority,
    Policies_in_force,
    Max_policies,
    Max_products,
    Lapse,
    Date_lapse,
    Payment,
    Type_risk,
    Second_driver,
    Premium,
    Cost_claims_year AS Cost_claims_current_year,
    N_claims_year AS N_claims_current_year,
    N_claims_history AS N_claims_total_history,
    R_Claims_history AS R_Claims_total_history, 
    Data_Version AS source_data_version,
    Processing_Timestamp AS source_processing_timestamp,
    Last_Modified AS source_last_modified,
    current_timestamp() AS dw_fact_load_timestamp
FROM
    workspace.default.motor_insurance_high_quality;