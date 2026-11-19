"""Resolve GMrepo MeSH disease codes to MONDO / HP CURIEs.

This is the single place where phenotype resolution happens. It writes:

  data/nodenorm_resolution.json   {mesh_code: {"id", "label"}}  -- Node Normalizer
                                  results only (no manual overrides)
  data/mesh_code_to_id.json       {mesh_code: curie}  -- Node Normalizer results plus
                                  HARDCODED_OVERRIDES; what phenotype_nodes.py and
                                  has_phenotype_edge.py read
  data/nodenorm_resolution.meta.json
                                  provenance: Babel release, query date, parameters,
                                  and every code that did not resolve

Method: query the Translator Node Normalizer (NCATS/RENCI) with MESH:<code>, and keep
the code only if the clique's primary identifier is a MONDO or HP CURIE. Anything else
(null, or a primary id from UMLS/DOID/NCIT/MESH/...) is left unresolved, because the
graph's Phenotype nodes must be MONDO/HP.

The result depends on the Babel release the API is serving, so the release is recorded
in the meta file. Rerunning later can legitimately change a few mappings.

Run after extract/extract_project_to_run_id_to_phenotypes.py and before
phenotype_nodes.py.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

HERE = Path(__file__).parent
REPO_ROOT = HERE.parent.parent

API_BASE = "https://nodenormalization-sri.renci.org"
BATCH_SIZE = 100
ACCEPTED_PREFIXES = ("MONDO", "HP")
QUERY_PARAMS = {
    "conflate": "true",
    "drug_chemical_conflate": "false",
    "description": "false",
    "individual_types": "false",
}

# 4 hardcoded overrides -- codes Node Normalizer couldn't resolve to MONDO/HP
# automatically, but which do have a real MONDO/HP concept once manually searched.
# Health and Healthy Aging intentionally collapse onto the same node (HP:0032322) --
# neither is a disease, and no aging-specific HPO/MONDO term exists.
# Order matters: phenotype_nodes.py keeps the original_name of the first code seen for
# a CURIE, so Health (D006262) must stay ahead of Healthy Aging (D000073656).
HARDCODED_OVERRIDES = {
    "D006262": {"id": "HP:0032322", "label": "Healthy"},
    "D000073656": {"id": "HP:0032322", "label": "Healthy"},
    "D007234": {"id": "HP:0001622", "label": "Premature birth"},
    "D000067011": {"id": "MONDO:0001371", "label": "protein-energy malnutrition"},
}


def distinct_disease_codes(phenotypes_parquet: Path) -> list[str]:
    df = pd.read_parquet(phenotypes_parquet, columns=["phenotypes"])
    codes = set()
    for val in df["phenotypes"]:
        for entry in json.loads(val):
            code = entry.get("disease")
            if code:
                codes.add(code)
    return sorted(codes)


def get_with_retry(url: str, params: dict, attempts: int = 5) -> requests.Response:
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == attempts - 1:
                raise
            time.sleep(2**attempt)


def query_node_normalizer(codes: list[str]) -> dict:
    """Returns {mesh_code: normalized-node dict, or None if unknown to the API}."""
    out = {}
    for i in range(0, len(codes), BATCH_SIZE):
        batch = codes[i : i + BATCH_SIZE]
        params = {"curie": [f"MESH:{c}" for c in batch], **QUERY_PARAMS}
        data = get_with_retry(f"{API_BASE}/get_normalized_nodes", params).json()
        missing = [c for c in batch if f"MESH:{c}" not in data]
        if missing:
            raise RuntimeError(f"Node Normalizer response is missing keys for: {missing}")
        for c in batch:
            out[c] = data[f"MESH:{c}"]
    return out


def main(phenotypes_parquet: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    status = get_with_retry(f"{API_BASE}/status", {}).json()
    babel_version = status.get("babel_version")
    print(f"Node Normalizer Babel release: {babel_version}")

    codes = distinct_disease_codes(phenotypes_parquet)
    print(f"{len(codes)} distinct MeSH disease codes in {phenotypes_parquet}")

    normalized = query_node_normalizer(codes)

    resolved = {}  # Node Normalizer results only
    unresolved = {}  # code -> why
    for code in codes:  # codes is sorted, so the output file is deterministic
        node = normalized[code]
        if node is None:
            unresolved[code] = "null (unknown to Node Normalizer)"
            continue
        primary = node["id"]["identifier"]
        if primary.split(":")[0] in ACCEPTED_PREFIXES:
            resolved[code] = {"id": primary, "label": node["id"].get("label")}
        else:
            unresolved[code] = f"primary id is {primary} ({node['id'].get('label')!r}), not MONDO/HP"

    # overrides: applied on top of the API results. Each override is meant to cover a
    # code the API can't resolve; flag any that the API now resolves differently.
    code_to_id = {code: info["id"] for code, info in resolved.items()}
    for code, info in HARDCODED_OVERRIDES.items():
        if code not in codes:
            print(f"WARNING: override {code} is not among the codes in the data")
        if code in resolved and resolved[code]["id"] != info["id"]:
            print(
                f"WARNING: override {code} -> {info['id']} replaces the API result "
                f"{resolved[code]['id']}"
            )
        code_to_id[code] = info["id"]
        unresolved.pop(code, None)

    with open(out_dir / "nodenorm_resolution.json", "w") as f:
        json.dump(resolved, f, indent=2)
    with open(out_dir / "mesh_code_to_id.json", "w") as f:
        json.dump(code_to_id, f, indent=2)
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "api": f"{API_BASE}/get_normalized_nodes",
        "babel_version": babel_version,
        "biolink_model": status.get("biolink_model", {}).get("tag"),
        "query_params": QUERY_PARAMS,
        "accepted_primary_id_prefixes": list(ACCEPTED_PREFIXES),
        "n_codes": len(codes),
        "n_resolved_by_api": len(resolved),
        "n_overrides": len(HARDCODED_OVERRIDES),
        "overrides": HARDCODED_OVERRIDES,
        "unresolved": unresolved,
    }
    with open(out_dir / "nodenorm_resolution.meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(f"resolved by Node Normalizer: {len(resolved)}")
    print(f"resolved by manual override: {len(HARDCODED_OVERRIDES)}")
    print(f"unresolved (dropped): {len(unresolved)}")
    for code, reason in unresolved.items():
        print(f"  {code}: {reason}")
    print(f"wrote {len(code_to_id)} mesh_code -> id mappings and resolution files to {out_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--phenotypes-parquet",
        type=Path,
        default=REPO_ROOT / "data/extracted/project_to_run_id_to_phenotypes.parquet",
    )
    parser.add_argument("--out-dir", type=Path, default=HERE / "data")
    args = parser.parse_args()
    main(args.phenotypes_parquet, args.out_dir)
