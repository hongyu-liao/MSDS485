#!/usr/bin/env python3
"""
Motor Insurance Data Processing Script (Modular Python Version)
- DataCleaning class handles all validation and fixes
- PIIGeneration class handles synthetic identity mapping
- Logging captures detailed fixes and issues
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime
from faker import Faker
from tqdm import tqdm


# === Logging Setup ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"data_changes_{datetime.now().strftime('%Y%m%d')}.log")

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='w'),
        logging.StreamHandler()
    ]
)


# === Class: DataCleaning ===
class DataCleaning:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.today = pd.Timestamp.today()
        self.current_year = self.today.year

    def _standardize_date(self, val, row_info, col_name):
        if pd.isna(val):
            return None
        val_str = str(val)
        if val_str.lower() in ["", "none", "nan", "nat", "null"]:
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y"):
            try:
                new_date = pd.to_datetime(val_str, format=fmt).strftime("%Y-%m-%d")
                if new_date != val_str:
                    logging.info(f"Standardized date in '{col_name}' for {row_info}: '{val_str}' -> '{new_date}'")
                return new_date
            except Exception:
                continue
        logging.info(f"Unparsable date in '{col_name}' for {row_info}: '{val_str}' replaced with None")
        return None

    def _safe_numeric(self, val, row_info, col_name, typ=float, allow_negative=True):
        try:
            num = typ(val)
            if not allow_negative and num < 0:
                logging.info(f"Removed negative value in '{col_name}' for {row_info}: {val} -> None")
                return None
            return abs(num) if typ == float and not allow_negative else num
        except:
            logging.info(f"Invalid numeric format in '{col_name}' for {row_info}: '{val}' -> None")
            return None

    def apply_cleaning(self):
        for idx, row in self.df.iterrows():
            row_info = f"ID={row.get('ID')}, Date_start_contract={row.get('Date_start_contract')}"

            # --- Date Fields ---
            for col in self.df.columns:
                if "Date" in col:
                    cleaned = self._standardize_date(row[col], row_info, col)
                    self.df.at[idx, col] = cleaned

            # --- Year_matriculation ---
            ymat = row.get("Year_matriculation")
            if isinstance(ymat, (int, float)):
                if ymat < 1900 or ymat > self.current_year:
                    logging.info(f"Out-of-range Year_matriculation for {row_info}: {ymat} -> None")
                    self.df.at[idx, "Year_matriculation"] = None
            elif pd.notna(ymat):
                logging.info(f"Invalid Year_matriculation format for {row_info}: {ymat} -> None")
                self.df.at[idx, "Year_matriculation"] = None

            # --- Date_birth / Age Check ---
            dob = row.get("Date_birth")
            try:
                dob_parsed = pd.to_datetime(dob)
                age = (self.today - dob_parsed).days / 365.25
                if age < 16 or age > 110:
                    logging.info(f"Unrealistic age from Date_birth for {row_info}: {dob} (age {age:.1f}) -> None")
                    self.df.at[idx, "Date_birth"] = None
            except:
                if pd.notna(dob):
                    logging.info(f"Invalid Date_birth for {row_info}: {dob} -> None")
                    self.df.at[idx, "Date_birth"] = None

            # --- Numeric Fields ---
            numeric_columns = {
                'Premium': float, 'Cost_claims_year': float, 'Value_vehicle': float,
                'Power': float, 'Cylinder_capacity': int, 'Weight': float,
                'Length': float, 'Seniority': int, 'Policies_in_force': int,
                'Max_policies': int, 'Max_products': int,
                'N_claims_year': int, 'N_claims_history': int
            }
            for col, typ in numeric_columns.items():
                if col in self.df.columns:
                    cleaned = self._safe_numeric(row[col], row_info, col, typ=typ, allow_negative=False)
                    self.df.at[idx, col] = cleaned

        # --- Cleanup blanks ---
        self.df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        return self.df


# === Class: PIIGeneration ===
class PIIGeneration:
    def __init__(self, df: pd.DataFrame, locale="en_US", seed=42):
        self.df = df.copy()
        self.faker = Faker(locale)
        Faker.seed(seed)

    def generate(self):
        unique_ids = sorted(self.df["ID"].dropna().unique())
        pii_records = []
        for uid in tqdm(unique_ids, desc="Generating synthetic PII"):
            pii_records.append({
                "ID": uid,
                "First_Name": self.faker.first_name(),
                "Last_Name": self.faker.last_name(),
                "ZipCode": self.faker.zipcode(),
                "LicensePlateNumber": self.faker.license_plate(),
                "SSN": self.faker.ssn()
            })
        df_pii = pd.DataFrame(pii_records)
        df_merged = self.df.merge(df_pii, on="ID", how="left")
        return df_merged


# === Main Execution ===
def main():
    input_csv = "raw_data.csv"
    output_csv = "processed_data.csv"

    print(f"Loading data from {input_csv} ...")
    df_raw = pd.read_csv(input_csv)
    print(f"Loaded {len(df_raw)} rows.")

    # Step 1: Clean Data
    cleaner = DataCleaning(df_raw)
    df_cleaned = cleaner.apply_cleaning()
    print("Data cleaning complete.")

    # Step 2: Add PII
    pii = PIIGeneration(df_cleaned)
    df_final = pii.generate()
    print("Synthetic PII generation complete.")

    # Step 3: Save
    df_final.to_csv(output_csv, index=False)
    print(f"Output saved to {output_csv}")

    # Step 4: Final flush
    for handler in logging.root.handlers[:]:
        handler.flush()
        handler.close()


# === Entry Point ===
if __name__ == "__main__":
    main()
