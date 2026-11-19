"""BioThings parser for GMrepo.

load_data(data_folder) does not re-implement the ETL: it runs stage_inputs.sh (links the
dump the dumper downloaded into the repo's data/raw/ and resets the rest of data/), then
run_etl.sh (streams the dump into parquet files -- no database server -- and runs the
repo's transform scripts in place). It then streams the ETL's parquet outputs from
<repo>/data/transformed as documents -- HAS_TAXON and HAS_PHENOTYPE edges in one stream,
each edge carrying its subject (sample) and object (taxon / phenotype) node properties.

Input in data_folder (written by the dumper, named from the download URL):
    gmrepo.sql.gz   -- really a ZIP archive holding one .sql file (the manifest sets
                       "uncompress": false, since the hub would try to gunzip it and fail);
                       extract/extract_from_dump.py reads it with zipfile
data_folder must not be inside the repo's own data/raw (stage_inputs.sh wipes that folder).

This module expects to live inside the GMrepo repo checkout (plugin/gmrepo/) so it can find
the scripts; set GMREPO_REPO to point at the checkout otherwise. The ETL's own
dependencies (pandas, pyarrow, requests, biothings_client) must be installed in the
interpreter running the hub.

Dev switches:
    GMREPO_SKIP_ETL=1        reuse the existing <repo>/data/transformed instead of
                             re-running everything (the transforms call live APIs, so a
                             full run takes a long time)
    GMREPO_ALLOW_WARNINGS=1  log, instead of fail on, the ETL's "could not be resolved"
                             warnings (see SILENT_FAILURE)
"""

from __future__ import annotations

import collections
import datetime
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

log = logging.getLogger("gmrepo_parser")

REPO = Path(os.environ.get("GMREPO_REPO") or Path(__file__).resolve().parents[2])
STAGE_SCRIPT = REPO / "stage_inputs.sh"
ETL_SCRIPT = REPO / "run_etl.sh"
BATCH_ROWS = 50_000

# Every node column is copied into each document (including a taxon's `lineage`).
# Documents hold nulls out: a property that is None/NaN is simply absent.

# Two ETL steps report a failed lookup only as a printed warning and still exit 0, so
# their output is watched for these messages:
#   taxon_nodes.py      "WARNING: N taxon ids could not be resolved at all" -- includes an
#                       NCBI rate-limit reply, which resolve_redirect() reads as "no match"
#   phenotype_nodes.py  "WARNING: N nodes have no original_name resolved"
SILENT_FAILURE = re.compile(r"could not be resolved at all|nodes have no original_name")


class GmrepoEtlError(RuntimeError):
    """Base class for everything this parser raises."""


class StagingError(GmrepoEtlError):
    """stage_inputs.sh failed: missing input, or data/ holds files it will not wipe."""


class EtlScriptError(GmrepoEtlError):
    """run_etl.sh (or a script it runs) exited nonzero."""


class SilentFailure(GmrepoEtlError):
    """The ETL exited 0 but reported lookups that failed (see SILENT_FAILURE)."""


class DanglingReference(GmrepoEtlError):
    """An edge points at a subject or object that is not in its node table."""


def _run_script(script: Path, args: list[str], error: type[GmrepoEtlError]) -> list[str]:
    """Run a bash script, logging each output line. Returns the lines that match
    SILENT_FAILURE; raises `error` if the script is missing or exits nonzero."""
    if not script.exists():
        raise error(f"{script} not found -- run from a GMrepo checkout or set GMREPO_REPO")

    env = {**os.environ, "GMREPO_PYTHON": sys.executable}
    proc = subprocess.Popen(
        ["bash", str(script), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    tail = collections.deque(maxlen=40)
    flagged = []
    for line in proc.stdout:
        line = line.rstrip("\n")
        tail.append(line)
        log.info("%s: %s", script.stem, line)
        if SILENT_FAILURE.search(line):
            flagged.append(line)
    if proc.wait() != 0:
        raise error(f"{script.name} exited {proc.returncode}; last output:\n" + "\n".join(tail))
    return flagged


def run_etl(data_folder: Path) -> None:
    _run_script(STAGE_SCRIPT, [str(data_folder)], StagingError)
    flagged = _run_script(ETL_SCRIPT, [], EtlScriptError)
    if flagged:
        message = "the ETL reported failed lookups:\n" + "\n".join(flagged[:5])
        if os.environ.get("GMREPO_ALLOW_WARNINGS"):
            log.warning("GMREPO_ALLOW_WARNINGS set, continuing. %s", message)
        else:
            raise SilentFailure(message)


def _present(value) -> bool:
    return value is not None and value == value  # drops None and NaN


def _clean(value):
    """Dates become 'YYYY-MM-DD' strings (what load/gen_csv does too): documents are JSON."""
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.strftime("%Y-%m-%d")
    return value


def _read_nodes(path: Path) -> dict:
    """{id: node dict without null properties}"""
    nodes = {}
    for row in pq.read_table(path).to_pylist():
        nodes[row["id"]] = {k: _clean(v) for k, v in row.items() if _present(v)}
    return nodes


def _iter_edges(path: Path, subjects: dict, objects: dict):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS):
        for row in batch.to_pylist():
            edge_id, subject_id, object_id = row.pop("id"), row.pop("subject"), row.pop("object")
            subject, obj = subjects.get(subject_id), objects.get(object_id)
            if subject is None or obj is None:
                raise DanglingReference(
                    f"edge {edge_id}: subject {subject_id!r} or object {object_id!r} is not in its node table"
                )
            yield {
                "_id": edge_id,
                "association": {k: _clean(v) for k, v in row.items() if _present(v)},
                "subject": dict(subject),
                "object": dict(obj),
            }


def load_data(data_folder):
    data_folder = Path(data_folder).resolve()
    out = REPO / "data" / "transformed"

    if os.environ.get("GMREPO_SKIP_ETL") and (out / "edges" / "has_taxon.parquet").exists():
        log.warning("GMREPO_SKIP_ETL set: reusing ETL output in %s", out)
    else:
        run_etl(data_folder)

    samples = _read_nodes(out / "nodes" / "sample.parquet")
    taxa = _read_nodes(out / "nodes" / "taxon.parquet")
    phenotypes = _read_nodes(out / "nodes" / "phenotype.parquet")

    counts = collections.Counter()
    # phenotype edges are the small table; taxon edges are millions of rows
    for doc in _iter_edges(out / "edges" / "has_phenotype.parquet", samples, phenotypes):
        counts[doc["association"]["predicate"]] += 1
        yield doc
    for doc in _iter_edges(out / "edges" / "has_taxon.parquet", samples, taxa):
        counts[doc["association"]["predicate"]] += 1
        yield doc
    log.info("GMrepo parse finished: %s", dict(counts))
