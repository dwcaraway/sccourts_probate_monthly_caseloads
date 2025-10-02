#!/usr/bin/env python3
"""
extract_caseloads.py

Reads all Excel files in a folder, finds South Carolina county blocks in column A,
extracts the 4 metrics (Pending first of month, Added, Disposed, Pending end of Month)
for months July through June (columns C..Q but ignoring columns F, J, N),
and writes a normalized JSON array with entries:
{ "year": <year>, "month": "<MonthName>", "county": "<County>", "metric": "<MetricType>", "value": <numeric_or_null> }

Usage:
    - Place this script in the folder with your Excel files (or set INPUT_FOLDER).
    - Run: python extract_caseloads.py
    - Output: caseloads_normalized.json
"""

import os
import csv
import re
from glob import glob

import pandas as pd

# --- Configuration ---
INPUT_FOLDER = os.path.join(os.path.dirname(__file__), 'excel')
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "caseloads_normalized.csv")

# Exact list of 46 South Carolina counties (used for exact matching in column A)
SC_COUNTIES = [
    "Abbeville","Aiken","Allendale","Anderson","Bamberg","Barnwell","Beaufort","Berkeley",
    "Calhoun","Charleston","Cherokee","Chester","Chesterfield","Clarendon","Colleton",
    "Darlington","Dillon","Dorchester","Edgefield","Fairfield","Florence","Georgetown",
    "Greenville","Greenwood","Hampton","Horry","Jasper","Kershaw","Lancaster","Laurens",
    "Lee","Lexington","Marion","Marlboro","McCormick","Newberry","Oconee","Orangeburg",
    "Pickens","Richland","Roswell","Saluda","Spartanburg","Sumter","Union","Williamsburg"
]

# NOTE: The above list must contain the exact county names as they appear in Column A.
# Adjust if the sheet uses a different naming convention (e.g., "County of Richland").

# Metric labels expected in Column B in the next four rows after the county row
EXPECTED_METRICS = [
    "Pending first of month",
    "Added",
    "Disposed",
    "Pending end of Month"
]

# Columns C..Q correspond to Excel column indexes 2..16 (0-based indexing)
# We must ignore columns F, J, and N which are Excel letters F(5), J(9), N(13) (0-based)
COLUMNNAMES = None  # will be derived per DataFrame if present; otherwise we use positional indices

# Create the month order corresponding to the retained columns (after ignoring F, J, N)
MONTH_ORDER = [
    "July","August","September","October","November","December",
    "January","February","March","April","May","June"
]

# Which zero-based column positions (relative to DataFrame columns) to use:
# We'll find the header columns by label if possible. Fallback to positional mapping: C..Q -> indices 2..16
# and then drop F(5), J(9), N(13) -> keep indices [2,3,4,6,7,8,10,11,12,14,15,16] (0-based)
FALLBACK_COL_POSITIONS = [2,3,4,6,7,8,10,11,12,14,15,16]

# Regex pattern to find the Period line and extract the two years
PERIOD_REGEX = re.compile(r"Period\s+0?7/0?1/(\d{4})\s+through\s+0?6/30/(\d{4})", re.IGNORECASE | re.DOTALL)

# --- Helpers ---


def find_period_years_in_first_rows(df: pd.DataFrame):
    """
    Try to extract year_start and year_end from the sheet's first 5 rows,
    which is expected to include the Period line like:
    South Carolina Court Administration
    Estate Workload Report
    Period 07/01/{year_starting} through 06/30/{year_ending}
    Returns (year_start:int, year_end:int) or None if not found.
    """
    # Search each of the first 5 rows individually first
    for row_idx in range(min(5, len(df))):
        try:
            row_text = df.iloc[row_idx].astype(str).str.cat(sep=" ")
            m = PERIOD_REGEX.search(row_text)
            if m:
                return int(m.group(1)), int(m.group(2))
        except Exception:
            continue
    
    # If no match found in individual rows, try concatenating all first 5 rows
    try:
        head_text = df.head(5).astype(str).agg(" ".join, axis=1).str.cat(sep=" ")
        m = PERIOD_REGEX.search(head_text)
        if m:
            return int(m.group(1)), int(m.group(2))
    except Exception:
        pass
    
    return None


def normalize_county_name(cell_value: str):
    if not isinstance(cell_value, str):
        return None
    s = cell_value.strip()
    # Remove trailing "County" if present and strip
    s = re.sub(r"\b[Cc]ounty\b\.?\,?$", "", s).strip()
    return s


def find_county_rows(df: pd.DataFrame):
    """
    Search Column A (first column) for county names. Return list of tuples (county_name, excel_row_number, df_row_index).
    Excel row number is 1-based (as in Excel view). df_row_index is 0-based DataFrame index location.
    """
    results = []
    first_col = df.columns[0]
    for idx, val in df[first_col].astype(str).items():
        name = normalize_county_name(val)
        if not name:
            continue
        # Case-insensitive exact match to one of the SC_COUNTIES
        for county in SC_COUNTIES:
            if name.lower() == county.lower():
                # compute Excel row number: DataFrame index may not be contiguous; use idx positional index
                # We'll get the positional row number via .index.get_loc if necessary. Simpler: row_number = df.index.get_loc(idx) + 1
                try:
                    row_pos = df.index.get_loc(idx)
                except Exception:
                    row_pos = idx
                excel_row = row_pos + 1
                results.append((county, excel_row, row_pos))
                break
    return results


def get_month_column_positions(df: pd.DataFrame):
    """
    Try to map months to column positions by searching rows 2 through 5 for the word "July".
    Once "July" is found, extract column positions that match the MONTH_ORDER entries.
    """
    for row_idx in range(1, min(5, len(df))):  # Rows 2 through 5 (0-based index is 1 through 4)
        row_values = df.iloc[row_idx].astype(str).str.strip().str.lower()
        try:
            # Find the first occurrence of "july" in the row
            start_col = row_values[row_values.str.contains("july", na=False)].index[0]
            # Extract positions for the MONTH_ORDER starting from "July"
            month_positions = []
            for month in MONTH_ORDER:
                month = month.lower()
                for col_idx in range(start_col, len(row_values)):
                    if month in str(row_values[col_idx]):
                        month_positions.append(col_idx)
                        break
            if len(month_positions) == 12:
                return month_positions
        except IndexError:
            continue

    print("Warning: Could not find matching months header, falling back")

    # Fallback to positional mapping if no match is found
    max_index = df.shape[1] - 1
    positions = [p for p in FALLBACK_COL_POSITIONS if p <= max_index]
    if len(positions) == 12:
        return positions
    # Last resort: try to find columns C..Q by ordinal indices 2..16 clipped to available columns
    fallback = [i for i in range(2, min(17, df.shape[1])) if i not in (5, 9, 13)]
    return fallback


def cell_to_number(v):
    """Convert a cell value to int/float if possible, otherwise None."""
    if pd.isna(v):
        return None
    if isinstance(v, (int, float)):
        return v
    s = str(v).strip().replace(",", "")
    if s == "":
        return None
    try:
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        try:
            return float(s)
        except Exception:
            return None


# --- Main extraction ---

all_entries = []

# TODO: remove the hardcoded file name
excel_files = glob(os.path.join(INPUT_FOLDER, "*2023_to_2024.xls*"))
if not excel_files:
    print("No Excel files found in", INPUT_FOLDER)

for filepath in sorted(excel_files):
    print("Processing:", filepath)
    try:
        # Read the first sheet only; preserve raw content (no header row inference)
        df = pd.read_excel(filepath, sheet_name=0, header=None, dtype=object)
    except Exception as e:
        print(f"  Failed to read {filepath}: {e}")
        continue

    # Attempt to find the Period years from the header rows
    years = find_period_years_in_first_rows(df)
    if not years:
        print("  Warning: Could not find Period line with years in the top rows; skipping file.")
        continue
    year_start, year_end = years

    # Determine month columns positions to extract (12 positions)
    month_cols = get_month_column_positions(df)
    if len(month_cols) != 12:
        print(f"  Warning: Expected 12 month columns but found {len(month_cols)}; continuing with detected columns.")

    county_rows = find_county_rows(df)
    if not county_rows:
        print("  No counties detected in this file (check Column A values).")
        continue

    # For each detected county, the next 4 rows (pos+1 .. pos+4) contain the metrics
    for county, excel_row, row_pos in county_rows:
        # The metric rows are row_pos + 1 .. +4 (0-based)
        metric_row_positions = [row_pos + i for i in range(1, 5)]
        # Ensure we don't exceed DataFrame bounds
        if metric_row_positions[-1] >= df.shape[0]:
            print(f"  Skipping county {county} at row {excel_row}: not enough rows for metrics.")
            continue

        # Read metric labels from Column B (DataFrame column index 1)
        metric_labels = []
        for r in metric_row_positions:
            raw_label = df.iat[r, 1] if df.shape[1] > 1 else None
            metric_labels.append(str(raw_label).strip() if raw_label is not None else "")

        # Map metric labels to expected metrics (attempt fuzzy/equality match)
        mapped_metrics = []
        for lbl in metric_labels:
            found = None
            for expected in EXPECTED_METRICS:
                if lbl.lower().startswith(expected.lower()) or expected.lower() in lbl.lower():
                    found = expected
                    break
            if not found:
                # fallback: accept the raw label if non-empty
                found = lbl if lbl else "Unknown Metric"
            mapped_metrics.append(found)

        # For each metric row, extract month values
        for metric_idx, r in enumerate(metric_row_positions):
            metric_type = mapped_metrics[metric_idx]
            for col_pos_idx, col in enumerate(month_cols):
                # Determine month name from MONTH_ORDER by position
                if col_pos_idx < len(MONTH_ORDER):
                    month = MONTH_ORDER[col_pos_idx]
                else:
                    month = f"Month_{col_pos_idx+1}"

                # Safely read the value cell
                try:
                    raw_value = df.iat[r, col]
                except Exception:
                    raw_value = None
                value = cell_to_number(raw_value)

                # Adjust year based on the month: July through December -> year_start, else -> year_end
                if month.lower() in ["july", "august", "september", "october", "november", "december"]:
                    year_for_entry = year_start
                else:
                    year_for_entry = year_end

                entry = {
                    "file": os.path.basename(filepath),
                    "year": year_for_entry,
                    "month": month,
                    "county": county,
                    "metric": metric_type,
                    "value": value
                }


                #TODO: remove debug
                if not(value):
                    print(f"    Debug: Skipping empty value for {county}, {metric_type}, {month}, year {year_for_entry}")
                    continue

                all_entries.append(entry)

# Write out CSV
# with open(OUTPUT_CSV, "w", encoding="utf-8", newline='') as out_f:
#     if all_entries:
#         # Get field names from the first entry
#         fieldnames = all_entries[0].keys()
#         writer = csv.DictWriter(out_f, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(all_entries)

print(f"Extraction complete. Wrote {len(all_entries)} entries to {OUTPUT_CSV}")
