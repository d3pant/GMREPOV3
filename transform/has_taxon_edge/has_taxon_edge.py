from pathlib import Path

import pandas as pd

SPECIES_ABUNDANCE_PARQUET = Path(__file__).parent.parent.parent / "data/extracted/species_abundance.parquet"
SAMPLE_PARQUET = Path(__file__).parent.parent.parent / "data/extracted/sample_to_run_info.parquet"
OUT_PATH = Path(__file__).parent.parent.parent / "data/transformed/edges/has_taxon.parquet"

abundance_df = pd.read_parquet(SPECIES_ABUNDANCE_PARQUET)

# species_abundance.accession_id is confirmed identical to sample_to_run_info.run_id
# (verified 100% match, 68,723/68,723 distinct accessions, 0 unmatched). Pull the
# fields needed for the join and edge properties from sample_to_run_info.
sample_df = pd.read_parquet(SAMPLE_PARQUET)[
    ["run_id", "second_sample_id", "project_id", "instrument_model"]
].drop_duplicates(subset="run_id")

merged = abundance_df.merge(
    sample_df, left_on="accession_id", right_on="run_id", how="inner"
)

# one edge per species_abundance row -- no deduplication. The same BioSample can
# report the same taxon across multiple runs (resequencing), and each such
# occurrence is its own distinct abundance measurement, kept as its own edge.
#
# taxon_id -1 (unclassified) is reported separately per rank (genus vs species
# are each independently ~100%-complete abundance distributions per run, not
# two halves of one number -- confirmed in EDA). taxon_nodes.py builds two
# separate Unclassified nodes for this reason (NCBITaxon:-1_genus /
# NCBITaxon:-1_species); route to the correct one using taxon_rank_level so a
# run's genus-unclassified and species-unclassified rows don't collide into one edge id.
UNCLASSIFIED_TAXID = -1

rows = []
for row in merged.itertuples(index=False):
    if row.ncbi_taxon_id == UNCLASSIFIED_TAXID:
        taxon_curie = f"NCBITaxon:{UNCLASSIFIED_TAXID}_{row.taxon_rank_level}"
        id_suffix = f"{UNCLASSIFIED_TAXID}_{row.taxon_rank_level}"
    else:
        taxon_curie = f"NCBITaxon:{row.ncbi_taxon_id}"
        id_suffix = str(row.ncbi_taxon_id)
    rows.append(
        {
            "id": f"{row.second_sample_id}_{row.run_id}_HAS_TAXON_{id_suffix}",
            "subject": row.second_sample_id,
            "object": taxon_curie,
            # category stays unprefixed here deliberately -- only `predicate` is
            # standardized on the biolink: prefix; see PRIMEDB's has_taxon_edge.py.
            "category": "MaterialSampleToOrganismTaxonAssociation",
            # biolink:-prefixed to match has_phenotype's predicate above and
            # PRIMEDB's, so the merged graph carries one predicate style.
            "predicate": "biolink:has_taxon",
            "BioProject": row.project_id,
            "sra_run_id": row.run_id,
            "relative_abundance": row.relative_abundance,
            "instrument": row.instrument_model,
            "primary_knowledge_source": ["infores:GMREPOV3"],
            "agent_type": "biolink:MANUAL_agent",
        }
    )

edge_df = pd.DataFrame(rows)
edge_df.to_parquet(OUT_PATH, index=False)

print(f"wrote {len(edge_df)} HAS_TAXON edges to {OUT_PATH}")
print(f"source species_abundance rows: {len(abundance_df)}")
print(f"unmatched rows (no sample_to_run_info join): {len(abundance_df) - len(merged)}")
