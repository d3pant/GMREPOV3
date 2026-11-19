#!/usr/bin/env bash
# Stage the input for the GMrepo ETL, which runs IN PLACE from this repo (see run_etl.sh).
#
#   stage_inputs.sh DATA_FOLDER
#
# DATA_FOLDER  folder holding the file the dumper downloaded: gmrepo.sql.gz
#              (really a ZIP archive holding gmdb_2025_working.sql)
#
# The ETL scripts read and write relative to where they live, so this script makes
# <repo>/data/ ready for them, from scratch, on every run:
#   data/raw/          a symlink to the downloaded dump (nothing is copied; the link is
#                      rebuilt each run, so the dated hub folder can be anything)
#   data/extracted/    empty
#   data/transformed/{nodes,edges}/   empty
# and deletes the files resolve_phenotypes.py generates in transform/phenotype/data/, so
# a stale mesh_code_to_id.json can never be read by phenotype_nodes.py or
# has_phenotype_edge.py. data/loaded/ and anything else under data/ is left alone.
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "usage: $0 DATA_FOLDER" >&2
    exit 2
fi

REPO="$(cd "$(dirname "$0")" && pwd -P)"
DATA_FOLDER="$(cd "$1" && pwd -P)"
DATA="$REPO/data"
PHENOTYPE_DATA_DIR="$REPO/transform/phenotype/data"
MARKER="$DATA/transformed/.gmrepo_etl_scratch"
DUMP_NAME="gmrepo.sql.gz"
GENERATED_JSON="nodenorm_resolution.json nodenorm_resolution.meta.json mesh_code_to_id.json"

# --- checks first: nothing is touched until all of them pass ------------------------------

if [ ! -f "$DATA_FOLDER/$DUMP_NAME" ]; then
    echo "missing input: $DATA_FOLDER/$DUMP_NAME" >&2
    echo "the folder holds: $(ls -A "$DATA_FOLDER" | tr '\n' ' ')" >&2
    exit 1
fi

# The folders below are wiped, so the download must not live inside them.
case "$DATA_FOLDER/" in
    "$DATA"/raw/*|"$DATA"/extracted/*|"$DATA"/transformed/*|"$PHENOTYPE_DATA_DIR"/*)
        echo "refusing: DATA_FOLDER $DATA_FOLDER is inside a folder this script wipes" >&2
        exit 2 ;;
esac

# Only wipe data/ that an earlier staging run made. Anything else in data/raw,
# data/extracted or data/transformed (hand-placed files, earlier outputs worth keeping)
# stops the run.
has_content() {  # the directory holds something besides .gitkeep / .DS_Store
    [ -d "$1" ] && [ -n "$(find "$1" -mindepth 1 ! -name .gitkeep ! -name .DS_Store -print | head -n 1)" ]
}
if [ ! -f "$MARKER" ]; then
    for d in raw extracted transformed; do
        if has_content "$DATA/$d"; then
            echo "refusing: $DATA/$d already holds files that were not staged by this script." >&2
            echo "Back them up, then delete data/raw, data/extracted and data/transformed and rerun." >&2
            exit 2
        fi
    done
fi

# --- stage -----------------------------------------------------------------------------------

for d in raw extracted; do
    mkdir -p "$DATA/$d"
    find "$DATA/$d" -mindepth 1 ! -name .gitkeep -delete
done
rm -rf "$DATA/transformed"
mkdir -p "$DATA/transformed/nodes" "$DATA/transformed/edges"
touch "$MARKER"

for f in $GENERATED_JSON; do
    rm -f "$PHENOTYPE_DATA_DIR/$f"
done

ln -s "$DATA_FOLDER/$DUMP_NAME" "$DATA/raw/$DUMP_NAME"

echo "staged $DATA from $DATA_FOLDER"
