import json
from pathlib import Path

import pandas as pd

SAMPLE_PARQUET = Path(__file__).parent.parent.parent / "data/extracted/sample_to_run_info.parquet"
CODE_TO_ID_PATH = Path(__file__).parent.parent / "phenotype/data/mesh_code_to_id.json"
OUT_PATH = Path(__file__).parent.parent.parent / "data/transformed/edges/has_phenotype.parquet"

with open(CODE_TO_ID_PATH) as f:
    mesh_code_to_id = json.load(f)

df = pd.read_parquet(SAMPLE_PARQUET)

# resolve each row's disease code to its Phenotype node CURIE. Empty disease (no
# phenotype recorded) and codes absent from mesh_code_to_id (unmapped/dropped
# codes) both resolve to None -- neither can produce an edge.
df["phenotype_curie"] = df["disease"].map(lambda c: mesh_code_to_id.get(c) if c else None)

resolved = df[df["phenotype_curie"].notnull()][["second_sample_id", "project_id", "phenotype_curie"]]

# a BioSample's runs occasionally disagree on disease (confirmed data quality
# issue in the source dump -- e.g. a case/control accession reused across two
# distinct subjects). Drop any BioSample with more than one distinct resolved
# phenotype rather than arbitrarily picking one.
curie_nunique = resolved.groupby("second_sample_id")["phenotype_curie"].nunique()
conflicting_ids = set(curie_nunique[curie_nunique > 1].index)

clean = resolved[~resolved["second_sample_id"].isin(conflicting_ids)]
clean = clean.drop_duplicates(subset="second_sample_id", keep="first")

rows = []
for row in clean.itertuples(index=False):
    numeric_id = row.phenotype_curie.split(":")[1]
    rows.append(
        {
            "id": f"{row.second_sample_id}_has_phenotype_{numeric_id}",
            "subject": row.second_sample_id,
            "object": row.phenotype_curie,
            "category": "biolink:MaterialSampleToDiseaseOrPhenotypicFeatureAssociation",
            "predicate": "biolink:has_phenotype",
            "BioProject": row.project_id,
            "primary_knowledge_source": ["infores:GMREPOV3"],
            "agent_type": "biolink:MANUAL_agent",
        }
    )

edge_df = pd.DataFrame(rows)
edge_df.to_parquet(OUT_PATH, index=False)

print(f"wrote {len(edge_df)} HAS_PHENOTYPE edges to {OUT_PATH}")
print(f"dropped {len(conflicting_ids)} BioSamples with conflicting resolved phenotypes")
print(f"total distinct BioSamples with a resolved phenotype: {resolved['second_sample_id'].nunique()}")
