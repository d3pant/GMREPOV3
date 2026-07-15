import subprocess
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent.parent / "data/loaded"
BULK_INSERT_BIN = Path(sys.executable).parent / "falkordb-bulk-insert"

GRAPH_NAME = "gmrepo"
SERVER_URL = "redis://127.0.0.1:6379"

cmd = [
    str(BULK_INSERT_BIN),
    GRAPH_NAME,
    "--server-url", SERVER_URL,
    "--enforce-schema",
    # Default escapechar (\) makes the CSV reader treat backslash specially,
    # corrupting the \" sequences inside our JSON-encoded ARRAY cells (e.g.
    # descriptions containing a literal double quote, like the dbGaP study
    # title 'Parkinson's Disease "The Gut Microbiome..."'). Disabling it
    # relies on plain CSV "" quote-doubling instead, which is what
    # pandas.to_csv already writes -- verified against all 5 CSVs with 0
    # ast.literal_eval failures.
    "--escapechar", "none",
    "--nodes-with-label", "Sample", str(DATA_DIR / "sample.csv"),
    "--nodes-with-label", "Taxon", str(DATA_DIR / "taxon.csv"),
    "--nodes-with-label", "Phenotype", str(DATA_DIR / "phenotype.csv"),
    "--relations-with-type", "HAS_PHENOTYPE", str(DATA_DIR / "has_phenotype.csv"),
    "--relations-with-type", "HAS_TAXON", str(DATA_DIR / "has_taxon.csv"),
    "--index", "Sample:id",
    "--index", "Taxon:id",
    "--index", "Phenotype:id",
]

subprocess.run(cmd, check=True)
