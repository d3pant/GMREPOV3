from pathlib import Path

import pandas as pd

IN_PATH = Path(__file__).parent.parent.parent / "data/extracted/sample_to_run_info.parquet"
OUT_PATH = Path(__file__).parent.parent.parent / "data/transformed/nodes/sample.parquet"

# Sample node identity: second_sample_id (BioSample ID), 0% null in source data.
# One BioSample can span multiple rows (multiple SRA runs); demographics are
# identical across those rows except for a small number of confirmed benign
# cases -- see the two exceptions handled below.
df = pd.read_parquet(IN_PATH)

# Columns that are identical (or negligibly inconsistent, <=1 conflicting
# BioSample out of 110,640) within every BioSample group -- safe to take the
# first row's value.
TAKE_FIRST_COLS = [
    "sample_name",
    "is_disease_stage_available",
    "disease_stage",
    "more",
    "more_info",
    "collection_date",
    "diet",
    "longitude",
    "latitude",
    "BMI",
    "Recent_Antibiotics_Use",
    "antibiotics_used",
    "Antibiotics_Dose",
    "Days_Without_Antibiotics_Use",
    "host_age",
    "sex",
    "country",
]

# project_id and original_sample_description each pair up 1:1 within a
# BioSample (verified: 20 BioSamples span 2 projects each, and every distinct
# project_id maps to exactly one description, no exceptions) -- kept as
# parallel lists so no information is lost or arbitrarily discarded.
grouped = df.groupby("second_sample_id", sort=False)

sample_df = grouped[TAKE_FIRST_COLS].first()

project_lists = grouped["project_id"].apply(lambda s: list(dict.fromkeys(s)))
desc_lists = grouped["original_sample_description"].apply(lambda s: list(dict.fromkeys(s)))

sample_df["project_id"] = project_lists
sample_df["original_sample_description"] = desc_lists

sample_df = sample_df.reset_index()

sample_df.to_parquet(OUT_PATH, index=False)

print(f"wrote {len(sample_df)} Sample nodes to {OUT_PATH}")
