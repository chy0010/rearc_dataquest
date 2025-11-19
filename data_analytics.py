#!/usr/bin/env python3
"""
data_analytics_full.py

Usage:
    python3 data_analytics_full.py

Requirements:
    pip install boto3 pandas

Functionality:
    - Reads time-series file s3://rearc-dataquest-quest/pr.data.0.Current
    - Reads population file s3://rearc-dataquest-quest/us_population.json
    - Cleans/trims whitespace
    - Computes:
        * Mean and Std Dev of population for years 2013-2018 (inclusive)
        * For each series_id, best year = year with max sum(value)
        * For series_id=PRS30006032 & period=Q01, join population for that year
    - Writes outputs locally and uploads them to s3://rearc-dataquest-quest/results/
"""

import boto3
import pandas as pd
import io
import json
import re
import sys
from typing import Any

# --------- Configuration ( set to bucket & keys) ----------
BUCKET = "rearc-dataquest-quest"
TS_KEY = "pr.data.0.Current"
POP_KEY = "us_population.json"
RESULTS_PREFIX = "results/"

# --------- boto3 client ----------
s3 = boto3.client("s3")

# --------- population JSON reader helpers ----------
def _normalize_json_obj_to_df(obj: Any) -> pd.DataFrame:
    if isinstance(obj, list):
        return pd.json_normalize(obj)
    if isinstance(obj, dict):
        # Detect dict-of-lists of equal length
        lengths = {k: len(v) if isinstance(v, (list, tuple)) else None for k, v in obj.items()}
        list_lengths = [l for l in lengths.values() if l is not None]
        if list_lengths:
            if len(set(list_lengths)) == 1:
                return pd.DataFrame(obj)
            else:
                max_len = max(list_lengths)
                records = []
                for i in range(max_len):
                    rec = {}
                    for k, v in obj.items():
                        if isinstance(v, (list, tuple)):
                            rec[k] = v[i] if i < len(v) else None
                        else:
                            rec[k] = v
                    records.append(rec)
                return pd.DataFrame(records)
        else:
            return pd.DataFrame([obj])
    return pd.json_normalize(obj)

def read_population_json(bytes_blob: bytes) -> pd.DataFrame:
    try:
        text = bytes_blob.decode("utf-8")
    except Exception:
        try:
            text = bytes_blob.decode("latin-1")
        except Exception:
            text = bytes_blob.decode("utf-8", errors="replace")

    preview = text[:2000].replace("\n", "\\n")
    print("==== population file preview (first 2000 chars) ====")
    print(preview)
    print("==== end preview ====\n")

    # Strategy 1: straight JSON
    try:
        obj = json.loads(text)
        return _normalize_json_obj_to_df(obj)
    except Exception as e_full:
        err_full = str(e_full)

    # Strategy 2: line-delimited JSON
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) > 1:
        try:
            objs = [json.loads(ln) for ln in lines]
            return pd.json_normalize(objs)
        except Exception as e_lines:
            err_lines = str(e_lines)

    # Strategy 3: regex extract first {...} or [...]
    m = re.search(r'(\{(?:[^{}]|(?1))*\}|\[(?:[^\[\]]|(?1))*\])', text, re.S)
    if m:
        candidate = m.group(1)
        try:
            obj = json.loads(candidate)
            return _normalize_json_obj_to_df(obj)
        except Exception as e_candidate:
            err_candidate = str(e_candidate)

    # Strategy 4: try CSV fallback if it looks comma-like
    sample_lines = text.strip().splitlines()[:20]
    comma_counts = [ln.count(",") for ln in sample_lines if ln.strip()]
    if len(comma_counts) >= 3 and len(set(comma_counts)) == 1:
        try:
            df_try = pd.read_csv(io.StringIO(text), dtype=False)
            return df_try
        except Exception as e_csv:
            err_csv = str(e_csv)

    # nothing worked -> raise with helpful info
    err_msg = "Failed to parse population JSON.\n"
    err_msg += f"json.loads error: {err_full}\n"
    if 'err_lines' in locals():
        err_msg += f"line-delimited parse error: {err_lines}\n"
    if 'err_candidate' in locals():
        err_msg += f"regex-candidate parse error: {err_candidate}\n"
    if 'err_csv' in locals():
        err_msg += f"csv parse attempt error: {err_csv}\n"
    raise RuntimeError(err_msg + "\nFile preview:\n" + preview)

# --------- Helpers ----------
def list_bucket_keys(bucket: str):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

def get_obj_bytes(bucket: str, key: str) -> bytes:
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()

def trim_all_strings(df: pd.DataFrame) -> pd.DataFrame:
    str_cols = df.select_dtypes(include=["object"]).columns
    for c in str_cols:
        df[c] = df[c].str.strip()
    return df

def upload_to_s3_bytes(bytes_data: bytes, bucket: str, key: str):
    s3.put_object(Bucket=bucket, Key=key, Body=bytes_data)
    print(f"Uploaded to s3://{bucket}/{key}")

# --------- Main pipeline ----------
def main():
    print("Listing keys in bucket...")
    keys = list_bucket_keys(BUCKET)
    print("\n".join(keys))
    print()

    # ---- Loading time-series file from S3 ----
    print(f"Fetching time-series: s3://{BUCKET}/{TS_KEY}")
    try:
        ts_bytes = get_obj_bytes(BUCKET, TS_KEY)
    except Exception as e:
        print("Failed to download time-series file:", e)
        sys.exit(1)

    # Trying to parse CSV with pandas sniffing common delimiters
    def read_time_series(bytes_blob: bytes) -> pd.DataFrame:
        try:
            text = bytes_blob.decode("utf-8")
        except Exception:
            text = bytes_blob.decode("latin-1", errors="replace")
        # Use pandas to guess delimiter by trying common ones
        for sep in [None, ',', '\t', '|', ';']:
            try:
                if sep is None:
                    # Let pandas try to infer (engine='python' allows sep=None)
                    df = pd.read_csv(io.StringIO(text), sep=None, engine="python", dtype=str)
                else:
                    df = pd.read_csv(io.StringIO(text), sep=sep, engine="python", dtype=str)
                print(f"Time-series read with sep={repr(sep)}; columns: {df.columns.tolist()}")
                return df
            except Exception:
                continue
        raise RuntimeError("Failed to parse time-series file with common delimiters.")

    df_ts = read_time_series(ts_bytes)

    # ---- Loading population JSON from S3 ----
    print(f"\nFetching population JSON: s3://{BUCKET}/{POP_KEY}")
    try:
        pop_bytes = get_obj_bytes(BUCKET, POP_KEY)
    except Exception as e:
        print("Failed to download population JSON:", e)
        sys.exit(1)

    df_pop = read_population_json(pop_bytes)

    # === Normalize population when top-level contains 'data' array ===
    if isinstance(df_pop, pd.DataFrame) and 'data' in df_pop.columns:
        # df_pop['data'] could be a list-like cell (single row); convert to proper DataFrame
        try:
            first = df_pop['data'].iloc[0]
            df_pop = pd.json_normalize(first)
        except Exception:
            all_items = []
            for cell in df_pop['data']:
                if isinstance(cell, list):
                    all_items.extend(cell)
            df_pop = pd.json_normalize(all_items)

    # ---- Clean/trimming and normalize column names ----
    df_ts.columns = [c.strip() for c in df_ts.columns]
    df_pop.columns = [c.strip() for c in df_pop.columns]

    df_ts = trim_all_strings(df_ts)
    df_pop = trim_all_strings(df_pop)

    # Normalize common column name variants to expected names
    # For time-series expected: series_id, year, period, value
    rename_map_ts = {}
    ts_cols_lower = {c.lower(): c for c in df_ts.columns}
    if "series_id" not in df_ts.columns:
        for alt in ("seriesid", "series", "series id"):
            if alt in ts_cols_lower:
                rename_map_ts[ts_cols_lower[alt]] = "series_id"
                break
    if "year" not in df_ts.columns:
        for alt in ("yr", "year"):
            if alt in ts_cols_lower:
                rename_map_ts[ts_cols_lower[alt]] = "year"
                break
    if "period" not in df_ts.columns:
        for alt in ("period", "periodid"):
            if alt in ts_cols_lower:
                rename_map_ts[ts_cols_lower[alt]] = "period"
                break
    if "value" not in df_ts.columns:
        for alt in ("value", "val", "amount"):
            if alt in ts_cols_lower:
                rename_map_ts[ts_cols_lower[alt]] = "value"
                break

    if rename_map_ts:
        df_ts = df_ts.rename(columns=rename_map_ts)

    # For population expected: year, population
    rename_map_pop = {}
    pop_cols_lower = {c.lower(): c for c in df_pop.columns}
    if "year" not in df_pop.columns:
        for alt in ("year", "yr"):
            if alt in pop_cols_lower:
                rename_map_pop[pop_cols_lower[alt]] = "year"
                break
    if "population" not in df_pop.columns:
        for alt in ("population", "pop", "population_count", "pop_count", "value"):
            if alt in pop_cols_lower:
                rename_map_pop[pop_cols_lower[alt]] = "population"
                break
    if rename_map_pop:
        df_pop = df_pop.rename(columns=rename_map_pop)

    # Additional explicit handling for capitalized names (from preview)
    if 'Year' in df_pop.columns and 'year' not in df_pop.columns:
        df_pop = df_pop.rename(columns={'Year': 'year'})
    if 'Population' in df_pop.columns and 'population' not in df_pop.columns:
        df_pop = df_pop.rename(columns={'Population': 'population'})
    if 'Nation' in df_pop.columns and 'Nation' not in df_pop.columns:
        # keep Nation as-is; we'll use it to filter United States row
        pass

    # Optional: filter to the United States rows only (use Nation or Nation ID)
    if 'Nation' in df_pop.columns:
        df_pop = df_pop[df_pop['Nation'].str.strip().str.lower() == 'united states'].copy()
    elif 'Nation ID' in df_pop.columns:
        df_pop = df_pop[df_pop['Nation ID'].str.strip().str.upper() == '01000US'].copy()

    # Final validation
    df_pop.columns = [c.strip() for c in df_pop.columns]
    if 'year' not in df_pop.columns or 'population' not in df_pop.columns:
        print("DEBUG: df_pop columns after normalization:", df_pop.columns.tolist())
        raise RuntimeError(f"population file must contain 'year' and 'population' after normalization. Found: {df_pop.columns.tolist()}")

    # cast numeric types
    df_ts['value'] = pd.to_numeric(df_ts['value'], errors='coerce')
    df_ts['year'] = pd.to_numeric(df_ts['year'], errors='coerce').astype('Int64')
    df_pop['population'] = pd.to_numeric(df_pop['population'], errors='coerce')
    df_pop['year'] = pd.to_numeric(df_pop['year'], errors='coerce').astype('Int64')

    # ---- Task 1: Population mean & stddev (2013-2018 inclusive) ---
    pop_subset = df_pop[(df_pop['year'] >= 2013) & (df_pop['year'] <= 2018)]
    mean_pop = pop_subset['population'].mean()
    std_pop = pop_subset['population'].std(ddof=0)  # population std (ddof=0)
    print("\nPopulation stats (2013-2018):")
    print(f"  Mean population: {mean_pop}")
    print(f"  StdDev population: {std_pop}")

    # ---- Task 2: For every series_id, find best year (sum of quarterly values) ---
    df_yearly = df_ts.dropna(subset=['series_id', 'year']).groupby(['series_id', 'year'], as_index=False)['value'].sum()
    df_yearly_sorted = df_yearly.sort_values(['series_id', 'value'], ascending=[True, False])
    df_best_year = df_yearly_sorted.drop_duplicates(subset=['series_id'], keep='first').reset_index(drop=True)
    print("\nBest year per series (sample 10 rows):")
    print(df_best_year.head(10).to_string(index=False))

    # ---- Task 3: For series_id=PRS30006032 and period=Q01, join population ----
    df_target = df_ts[(df_ts['series_id'] == 'PRS30006032') & (df_ts['period'] == 'Q01')].copy()
    df_target['year'] = pd.to_numeric(df_target['year'], errors='coerce').astype('Int64')
    df_joined = df_target.merge(df_pop, on='year', how='left')[['series_id', 'year', 'period', 'value', 'population']]
    df_joined = df_joined.rename(columns={'population': 'Population'})
    print("\nJoined report for PRS30006032 Q01:")
    if df_joined.empty:
        print("  No rows found for series_id=PRS30006032 and period=Q01")
    else:
        print(df_joined.to_string(index=False))

    # ---- Save outputs locally ----
    out_best = 'best_year_per_series.csv'
    out_popstats = 'population_stats_2013_2018.txt'
    out_joined = 'PRS30006032_Q01_joined.csv'

    df_best_year.to_csv(out_best, index=False)
    with open(out_popstats, 'w') as f:
        f.write(f"Mean_population_2013_2018,{mean_pop}\nStdDev_population_2013_2018,{std_pop}\n")
    df_joined.to_csv(out_joined, index=False)

    print(f"\nWrote files locally: {out_best}, {out_popstats}, {out_joined}")

    # ---- Upload to S3 results/ prefix ----
    try:
        upload_to_s3_bytes(open(out_best, 'rb').read(), BUCKET, RESULTS_PREFIX + out_best)
        upload_to_s3_bytes(open(out_popstats, 'rb').read(), BUCKET, RESULTS_PREFIX + out_popstats)
        upload_to_s3_bytes(open(out_joined, 'rb').read(), BUCKET, RESULTS_PREFIX + out_joined)
    except Exception as e:
        print("Warning: failed to upload results to S3:", e)

    print("\nDone.")

if __name__ == "__main__":
    main()
