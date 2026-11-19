import json
from collections import Counter
from pathlib import Path

import pandas as pd

CODE_TO_ID_PATH = Path(__file__).parent / "data/mesh_code_to_id.json"
PHENOTYPES_PARQUET = Path(__file__).parent.parent.parent / "data/extracted/project_to_run_id_to_phenotypes.parquet"
MESH_PARQUET = Path(__file__).parent.parent.parent / "data/extracted/mesh_data.parquet"
OUT_PATH = Path(__file__).parent.parent.parent / "data/transformed/nodes/phenotype.parquet"

# mesh_code -> MONDO/HP CURIE, produced by resolve_phenotypes.py (live Node Normalizer
# lookups plus the manual overrides). Run that first; this script does no resolution.
with open(CODE_TO_ID_PATH) as f:
    code_to_id = json.load(f)

# original_name: mesh_data.term is the authoritative source. A handful of codes
# (newer disease classifications) aren't in mesh_data at all -- fall back to the
# majority-vote term text from the phenotypes JSON column for those.
codes = set(code_to_id)
mesh_df = pd.read_parquet(MESH_PARQUET, columns=["uid", "term"])
mesh_df = mesh_df[mesh_df["uid"].isin(codes)]
mesh_lookup = dict(zip(mesh_df["uid"], mesh_df["term"]))

phenotypes_df = pd.read_parquet(PHENOTYPES_PARQUET)
term_counts_by_code = {}
for val in phenotypes_df["phenotypes"]:
    for entry in json.loads(val):
        code = entry.get("disease")
        term = entry.get("term")
        if code and term:
            term_counts_by_code.setdefault(code, Counter())[term] += 1


def original_name_for(code: str) -> str:
    if code in mesh_lookup:
        return mesh_lookup[code]
    if code in term_counts_by_code:
        return term_counts_by_code[code].most_common(1)[0][0]
    return None


# Multiple mesh codes can resolve to the same CURIE (either our intentional Health /
# Healthy Aging collapse, or Node Normalizer independently recognizing pre-existing
# MeSH synonym pairs, e.g. "Coronary Artery Disease" and "Coronary Disease"). A node
# table must have exactly one row per distinct id -- dedupe here, keeping the
# original_name from whichever mesh code was encountered first.
curie_to_codes = {}
curie_to_name = {}
for code, curie in code_to_id.items():
    curie_to_codes.setdefault(curie, []).append(code)
    if curie not in curie_to_name:
        curie_to_name[curie] = original_name_for(code)

rows = []
for curie, name in curie_to_name.items():
    rows.append(
        {
            "id": curie,
            "original_name": name,
            "category": "biolink:DiseaseOrPhenotypicFeature",
        }
    )

phenotype_df = pd.DataFrame(rows)
phenotype_df.to_parquet(OUT_PATH, index=False)

print(f"wrote {len(phenotype_df)} Phenotype nodes to {OUT_PATH}")

missing_name = phenotype_df["original_name"].isnull().sum()
if missing_name:
    print(f"WARNING: {missing_name} nodes have no original_name resolved")

collapsed = {curie: codes for curie, codes in curie_to_codes.items() if len(codes) > 1}
if collapsed:
    print(f"\n{len(collapsed)} nodes have multiple source mesh codes:")
    for curie, codes in collapsed.items():
        print(f"  {curie}: {codes}")
