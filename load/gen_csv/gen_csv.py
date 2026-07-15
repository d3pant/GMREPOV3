import json
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
NODES_IN = DATA_DIR / "transformed/nodes"
EDGES_IN = DATA_DIR / "transformed/edges"
OUT_DIR = DATA_DIR / "loaded"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# FalkorDB bulk-loader (--enforce-schema) header conventions, verified against
# falkordb_bulk_loader/entity_file.py, label.py, relation_type.py:
#   - node CSVs: exactly one column typed ID (or ID(NAMESPACE)); every other
#     column is name:TYPE where TYPE in {STRING, DOUBLE, LONG, BOOL, ARRAY}
#   - edge CSVs: exactly one START_ID column and one END_ID column (these are
#     never stored as properties regardless of the name given); every other
#     column is name:TYPE as above
#   - an empty CSV cell is read as NULL for any type; ARRAY cells must be a
#     Python-literal list string (e.g. "['a', 'b']"), parsed via ast.literal_eval


def write_csv(df: pd.DataFrame, header: list[str], out_path: Path) -> None:
    out_df = df.copy()
    out_df.columns = header
    out_df.to_csv(out_path, index=False, na_rep="")
    print(f"wrote {len(out_df)} rows to {out_path}")


def array_col_to_literal_str(series: pd.Series) -> pd.Series:
    # json.dumps (not str()) so values containing apostrophes -- e.g.
    # "Fecal sample from Crohn's disease patient CD11" (2,114 rows in
    # original_sample_description) -- don't break the loader's
    # ast.literal_eval parse. JSON double-quoted strings are valid Python
    # literal syntax, and json.dumps only ever needs to escape ", never '.
    #
    # Drop None entries (676 rows in original_sample_description are the
    # singleton [None] -- no description recorded at all for that BioSample)
    # -- json.dumps([None]) is "[null]", which is valid JSON but not valid
    # Python literal syntax, so ast.literal_eval would reject it. An absent
    # description is better modeled as an empty array than a null placeholder.
    return series.apply(lambda lst: json.dumps([x for x in lst if x is not None]))


# ---------------------------------------------------------------------------
# Sample nodes
# ---------------------------------------------------------------------------
sample_df = pd.read_parquet(NODES_IN / "sample.parquet")

sample_df["collection_date"] = sample_df["collection_date"].dt.strftime("%Y-%m-%d")
sample_df["project_id"] = array_col_to_literal_str(sample_df["project_id"])
sample_df["original_sample_description"] = array_col_to_literal_str(
    sample_df["original_sample_description"]
)

sample_header = [
    "id:ID",
    "sample_name:STRING",
    "is_disease_stage_available:STRING",
    "disease_stage:STRING",
    "more:STRING",
    "more_info:STRING",
    "collection_date:STRING",
    "diet:STRING",
    "longitude:DOUBLE",
    "latitude:DOUBLE",
    "BMI:DOUBLE",
    "Recent_Antibiotics_Use:STRING",
    "antibiotics_used:STRING",
    "Antibiotics_Dose:STRING",
    "Days_Without_Antibiotics_Use:STRING",
    "host_age:DOUBLE",
    "sex:STRING",
    "country:STRING",
    "project_id:ARRAY",
    "original_sample_description:ARRAY",
]
write_csv(sample_df, sample_header, OUT_DIR / "sample.csv")

# ---------------------------------------------------------------------------
# Taxon nodes
# ---------------------------------------------------------------------------
taxon_df = pd.read_parquet(NODES_IN / "taxon.parquet")

# parent_taxid is float64 only because of the two Unclassified sentinel rows
# (NaN); render every real value as an integer string, sentinel rows as "".
taxon_df["parent_taxid"] = taxon_df["parent_taxid"].apply(
    lambda v: "" if pd.isnull(v) else str(int(v))
)

taxon_header = [
    "id:ID",
    "taxid:LONG",
    "name:STRING",
    "parent_taxid:LONG",
    "lineage:STRING",
    "rank:STRING",
    "category:STRING",
]
write_csv(taxon_df, taxon_header, OUT_DIR / "taxon.csv")

# ---------------------------------------------------------------------------
# Phenotype nodes
# ---------------------------------------------------------------------------
phenotype_df = pd.read_parquet(NODES_IN / "phenotype.parquet")

phenotype_header = [
    "id:ID",
    "original_name:STRING",
    "category:STRING",
]
write_csv(phenotype_df, phenotype_header, OUT_DIR / "phenotype.csv")

# ---------------------------------------------------------------------------
# HAS_PHENOTYPE edges
# ---------------------------------------------------------------------------
has_phenotype_df = pd.read_parquet(EDGES_IN / "has_phenotype.parquet")

has_phenotype_df["primary_knowledge_source"] = array_col_to_literal_str(
    has_phenotype_df["primary_knowledge_source"]
)

has_phenotype_header = [
    "id:STRING",
    ":START_ID",
    ":END_ID",
    "category:STRING",
    "predicate:STRING",
    "BioProject:STRING",
    "primary_knowledge_source:ARRAY",
    "agent_type:STRING",
]
write_csv(has_phenotype_df, has_phenotype_header, OUT_DIR / "has_phenotype.csv")

# ---------------------------------------------------------------------------
# HAS_TAXON edges
# ---------------------------------------------------------------------------
has_taxon_df = pd.read_parquet(EDGES_IN / "has_taxon.parquet")

has_taxon_df["primary_knowledge_source"] = array_col_to_literal_str(
    has_taxon_df["primary_knowledge_source"]
)

has_taxon_header = [
    "id:STRING",
    ":START_ID",
    ":END_ID",
    "category:STRING",
    "predicate:STRING",
    "BioProject:STRING",
    "sra_run_id:STRING",
    "relative_abundance:DOUBLE",
    "instrument:STRING",
    "primary_knowledge_source:ARRAY",
    "agent_type:STRING",
]
write_csv(has_taxon_df, has_taxon_header, OUT_DIR / "has_taxon.csv")
