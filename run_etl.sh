#!/usr/bin/env bash
# Run the GMrepo ETL, one script after another, IN PLACE in this repo.
#
#   stage_inputs.sh DATA_FOLDER   # 1. links the downloaded dump into data/raw/
#   run_etl.sh                    # 2. this: streams the dump into data/extracted/, then runs
#                                 #    the transforms; results land in data/transformed/
#
# (the BioThings parser, parser.py, does both itself.)
#
# The scripts read and write relative to where they live (data/extracted,
# data/transformed), so this overwrites this repo's data/ -- which is why it refuses to run
# unless stage_inputs.sh has prepared data/ (it leaves a marker in data/transformed/).
#
# Set GMREPO_PYTHON to pick the interpreter (default: python3).
set -euo pipefail

REPO="$(cd "$(dirname "$0")" && pwd -P)"
PY="${GMREPO_PYTHON:-python3}"

if [ ! -f "$REPO/data/transformed/.gmrepo_etl_scratch" ]; then
    echo "refusing: data/ was not staged -- run stage_inputs.sh DATA_FOLDER first" >&2
    exit 2
fi

run() {
    echo
    echo "=== $1 ==="
    "$PY" "$REPO/$1"
}

run extract/extract_from_dump.py
run transform/sample/sample_nodes.py
run transform/taxon/taxon_nodes.py
run transform/phenotype/resolve_phenotypes.py
run transform/phenotype/phenotype_nodes.py
run transform/has_taxon_edge/has_taxon_edge.py
run transform/has_phenotype_edge/has_phenotype_edge.py

echo
echo "=== ETL finished: outputs in $REPO/data/transformed ==="
