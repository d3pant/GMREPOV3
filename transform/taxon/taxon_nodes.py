import re
import time
from pathlib import Path
from typing import Optional

import biothings_client as bt
import pandas as pd
import requests

IN_PATH = Path(__file__).parent.parent.parent / "data/extracted/species_abundance.parquet"
OUT_PATH = Path(__file__).parent.parent.parent / "data/transformed/nodes/taxon.parquet"

UNCLASSIFIED_TAXID = -1

FIELDS = ["scientific_name", "parent_taxid", "lineage", "rank"]


def resolve_redirect(taxid: int) -> Optional[int]:
    """Follow NCBI's merge/retirement redirect for a taxid that Biothings can't find."""
    r = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        params={"db": "taxonomy", "id": taxid, "retmode": "xml"},
    )
    m = re.search(r"<TaxId>(\d+)</TaxId>", r.text)
    if not m:
        return None
    new_id = int(m.group(1))
    return new_id if new_id != taxid else None


df = pd.read_parquet(IN_PATH)

distinct_taxids = df.loc[df["ncbi_taxon_id"] != UNCLASSIFIED_TAXID, "ncbi_taxon_id"].unique().tolist()

client = bt.get_client("taxon")
infos = client.gettaxa(distinct_taxids, fields=FIELDS)

# original_taxid -> info dict, keyed by the taxid actually present in species_abundance
# (not the redirected one), so this table still joins cleanly against the source data.
resolved = {}
unresolved_original_ids = []
for info in infos:
    original_taxid = int(info["query"])
    if info.get("notfound"):
        unresolved_original_ids.append(original_taxid)
    else:
        resolved[original_taxid] = info

# fall back to NCBI's merge/retirement redirect for anything Biothings couldn't find,
# then re-resolve the redirected (current) id via Biothings.
redirect_map = {}
for taxid in unresolved_original_ids:
    new_id = resolve_redirect(taxid)
    if new_id is not None:
        redirect_map[taxid] = new_id
    time.sleep(0.35)  # be polite to NCBI's rate limit

if redirect_map:
    redirected_infos = client.gettaxa(list(set(redirect_map.values())), fields=FIELDS)
    redirected_by_id = {int(i["query"]): i for i in redirected_infos if not i.get("notfound")}
    for original_taxid, new_id in redirect_map.items():
        if new_id in redirected_by_id:
            resolved[original_taxid] = redirected_by_id[new_id]

rows = []
for taxid, info in resolved.items():
    rows.append(
        {
            "id": f"NCBITaxon:{taxid}",
            "taxid": taxid,
            "name": info.get("scientific_name"),
            "parent_taxid": info.get("parent_taxid"),
            "lineage": info.get("lineage"),
            "rank": info.get("rank"),
            "category": "biolink:OrganismTaxon",
        }
    )

# -1 "unclassified" sentinel: one node per rank, not one shared node. GMrepo
# reports the unclassified fraction separately for genus-level and species-level
# calling (each rank's abundances independently sum to ~100% per run, confirmed
# in EDA) -- collapsing both into a single node made every (run, -1) edge id
# collide across the two ranks. Rank here comes directly from
# species_abundance.taxon_rank_level (the source table), NOT Biothings, since
# Biothings correctly has no entry for a sentinel id.
for rank in ["genus", "species"]:
    rows.append(
        {
            "id": f"NCBITaxon:{UNCLASSIFIED_TAXID}_{rank}",
            "taxid": UNCLASSIFIED_TAXID,
            "name": f"Unclassified ({rank})",
            "parent_taxid": None,
            "lineage": None,
            "rank": rank,
            "category": "biolink:OrganismTaxon",
        }
    )

taxon_df = pd.DataFrame(rows)
taxon_df.to_parquet(OUT_PATH, index=False)

still_unresolved = set(unresolved_original_ids) - set(resolved.keys())
print(f"wrote {len(taxon_df)} Taxon nodes to {OUT_PATH}")
if still_unresolved:
    print(f"WARNING: {len(still_unresolved)} taxon ids could not be resolved at all: {sorted(still_unresolved)}")
