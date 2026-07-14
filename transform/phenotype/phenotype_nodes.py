import json
from collections import Counter
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

RESOLUTION_PATH = Path(__file__).parent / "data/nodenorm_resolution.json"
PHENOTYPES_PARQUET = Path(__file__).parent.parent.parent / "data/extracted/project_to_run_id_to_phenotypes.parquet"
OUT_PATH = Path(__file__).parent.parent.parent / "data/transformed/nodes/phenotype.parquet"

engine = create_engine("mysql+pymysql://root:gmrepo@127.0.0.1:3307/gmrepo")

# 262 codes resolved directly by NCBI Node Normalizer to MONDO/HP (see project memory
# for full resolution methodology). Precomputed once; not re-queried here.
with open(RESOLUTION_PATH) as f:
    resolved = json.load(f)

# 4 hardcoded overrides -- codes Node Normalizer couldn't resolve automatically, but
# which do have a real MONDO/HP concept once manually searched. Health and Healthy
# Aging intentionally collapse onto the same node (HP:0032322) -- neither is a
# disease, and no aging-specific HPO/MONDO term exists.
HARDCODED_OVERRIDES = {
    "D006262": {"id": "HP:0032322", "label": "Healthy"},
    "D000073656": {"id": "HP:0032322", "label": "Healthy"},
    "D007234": {"id": "HP:0001622", "label": "Premature birth"},
    "D000067011": {"id": "MONDO:0001371", "label": "protein-energy malnutrition"},
}

resolved.update(HARDCODED_OVERRIDES)

# original_name: mesh_data.term is the authoritative source. A handful of codes
# (newer disease classifications) aren't in mesh_data at all -- fall back to the
# majority-vote term text from the phenotypes JSON column for those.
codes = list(resolved.keys())
placeholders = ",".join(f"'{c}'" for c in codes)
mesh_df = pd.read_sql(f"SELECT uid, term FROM mesh_data WHERE uid IN ({placeholders})", engine)
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
for code, info in resolved.items():
    curie = info["id"]
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

# mesh_code -> id lookup, needed later for building HAS_PHENOTYPE edges. Kept
# separate from the node table, which should only ever contain node identity/properties.
CODE_TO_ID_PATH = Path(__file__).parent / "data/mesh_code_to_id.json"
code_to_id = {code: info["id"] for code, info in resolved.items()}
with open(CODE_TO_ID_PATH, "w") as f:
    json.dump(code_to_id, f, indent=2)
print(f"wrote {len(code_to_id)} mesh_code -> id mappings to {CODE_TO_ID_PATH}")

collapsed = {curie: codes for curie, codes in curie_to_codes.items() if len(codes) > 1}
if collapsed:
    print(f"\n{len(collapsed)} nodes have multiple source mesh codes:")
    for curie, codes in collapsed.items():
        print(f"  {curie}: {codes}")
