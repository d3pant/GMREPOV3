"""Stream the GMrepo SQL dump into parquet files -- no database server needed.

    python extract/extract_from_dump.py [DUMP_ZIP] [--out-dir DIR]

DUMP_ZIP defaults to data/raw/gmrepo.sql.gz (really a ZIP archive holding one .sql file,
a Navicat MySQL export). The .sql is read straight out of the zip, line by line, never
unpacked to disk or held in memory. In it every row is its own single-line
`INSERT INTO `table` VALUES (...);` statement, and each table's `CREATE TABLE` block comes
before its rows, so one pass is enough: column names and types are read from the CREATE
TABLE block, and the INSERT lines of the tables listed in TABLES are parsed and written
to <out-dir>/<table>.parquet. Every other table in the dump is skipped without parsing.

Column types come from the CREATE TABLE block:
    int/bigint/...       int64   (NULL stays null; pandas reads it as float64 when a
                                  column has NULLs, int64 when it has none -- exactly
                                  what pandas.read_sql gave)
    double/float         float64
    datetime/timestamp   timestamp[ns]
    varchar/char/text    string
Any type or syntax this parser does not know raises an error instead of guessing.

Reading the zip member to its end also verifies the archive's CRC, so a truncated or
corrupt download fails here rather than yielding partial tables.
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
DEFAULT_DUMP = REPO / "data" / "raw" / "gmrepo.sql.gz"
DEFAULT_OUT_DIR = REPO / "data" / "extracted"

# Tables the ETL needs. Each becomes <out-dir>/<table>.parquet.
TABLES = (
    "project_to_run_id_to_phenotypes",
    "sample_to_run_info",
    "species_abundance",
    "mesh_data",
)
BATCH_ROWS = 200_000


class DumpError(RuntimeError):
    """The dump is not shaped the way this parser expects."""


# --- CREATE TABLE ------------------------------------------------------------------------

_COLUMN = re.compile(r"^\s+`([^`]+)`\s+([a-zA-Z]+)")  # "  `name` type..." inside CREATE TABLE
_INT_TYPES = {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}
_FLOAT_TYPES = {"double", "float", "real"}
_TEXT_TYPES = {"varchar", "char", "text", "tinytext", "mediumtext", "longtext"}
_DATETIME_TYPES = {"datetime", "timestamp"}


def _column_type(table: str, column: str, sql_type: str):
    """(pyarrow type, converter from the raw token text to a python value)"""
    t = sql_type.lower()
    if t in _INT_TYPES:
        return pa.int64(), int
    if t in _FLOAT_TYPES:
        return pa.float64(), float
    if t in _DATETIME_TYPES:
        return pa.timestamp("ns"), _parse_datetime
    if t in _TEXT_TYPES:
        return pa.string(), None  # strings are already text after unescaping
    raise DumpError(f"{table}.{column}: unsupported column type {sql_type!r}")


def _parse_datetime(text: str) -> datetime:
    if text.startswith("0000-00-00"):
        raise DumpError(f"zero date {text!r} cannot be represented")
    return datetime.fromisoformat(text)


# --- INSERT values -----------------------------------------------------------------------

# One value then the `,` or `)` that follows it:
#   group 1 quoted string   group 2 NULL   group 3 number
_VALUE = re.compile(
    r"""'((?:[^'\\]|\\.|'')*)'      # 'string', with \x escapes and '' for a quote
      | (NULL)
      | (-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)
    """,
    re.VERBOSE | re.DOTALL,
)
_ESCAPES = {"0": "\0", "'": "'", '"': '"', "b": "\b", "n": "\n", "r": "\r", "t": "\t", "Z": "\x1a", "\\": "\\"}
_UNESCAPE = re.compile(r"\\(.)|''", re.DOTALL)


def _unescape_one(m: re.Match) -> str:
    if m.group(0) == "''":
        return "'"
    c = m.group(1)
    # MySQL: an unknown \x escape is just x (so \% and \_ keep their backslash only in LIKE)
    return _ESCAPES.get(c, c)


def _unescape(s: str) -> str:
    if "\\" not in s and "''" not in s:
        return s
    return _UNESCAPE.sub(_unescape_one, s)


def _parse_insert(line: str, prefix_len: int, n_columns: int, where: str):
    """Yield each row (a list of raw values: str | None | number text) of one INSERT line.

    Rows are tuples after `VALUES`; a statement may hold several, separated by commas.
    """
    pos = prefix_len
    value = _VALUE.match
    while True:
        if line[pos : pos + 1] != "(":
            raise DumpError(f"{where}: expected '(' at column {pos}")
        pos += 1
        row = []
        while True:
            m = value(line, pos)
            if m is None:
                raise DumpError(f"{where}: cannot parse a value at column {pos}: {line[pos:pos + 60]!r}")
            if m.group(1) is not None:
                row.append(("s", _unescape(m.group(1))))
            elif m.group(2) is not None:
                row.append(None)
            else:
                row.append(("n", m.group(3)))
            pos = m.end()
            sep = line[pos : pos + 1]
            pos += 1
            if sep == ",":
                if line[pos : pos + 1] == " ":
                    pos += 1
                continue
            if sep == ")":
                break
            raise DumpError(f"{where}: expected ',' or ')' at column {pos - 1}")
        if len(row) != n_columns:
            raise DumpError(f"{where}: row has {len(row)} values, the table has {n_columns} columns")
        yield row
        sep = line[pos : pos + 1]
        if sep == ";":
            return
        if line[pos : pos + 2] != ", ":
            raise DumpError(f"{where}: expected ';' or ', (' after a row at column {pos}")
        pos += 2


# --- one table ------------------------------------------------------------------------------


class TableWriter:
    """Collects the rows of one table and writes them to <path> in batches."""

    def __init__(self, name: str, columns: list[tuple[str, str]], path: Path):
        self.name = name
        self.names = [c for c, _ in columns]
        types = [_column_type(name, c, t) for c, t in columns]
        self.schema = pa.schema([pa.field(c, ty) for (c, _), (ty, _) in zip(columns, types)])
        self.converters = [conv for _, conv in types]
        self.path = path
        self.rows = 0
        self._cols: list[list] = [[] for _ in columns]
        self._writer = pq.ParquetWriter(path, self.schema)

    def add(self, row: list) -> None:
        for i, (cell, conv) in enumerate(zip(row, self.converters)):
            if cell is None:
                value = None
            else:
                kind, text = cell
                if conv is None:
                    if kind != "s":
                        raise DumpError(f"{self.name}.{self.names[i]}: expected a string, got {text!r}")
                    value = text
                elif conv is _parse_datetime:
                    value = conv(text)
                else:
                    if kind != "n":
                        raise DumpError(f"{self.name}.{self.names[i]}: expected a number, got {text!r}")
                    value = conv(text)
            self._cols[i].append(value)
        self.rows += 1
        if len(self._cols[0]) >= BATCH_ROWS:
            self._flush()

    def _flush(self) -> None:
        if not self._cols[0]:
            return
        arrays = [pa.array(col, type=f.type) for col, f in zip(self._cols, self.schema)]
        self._writer.write_table(pa.Table.from_arrays(arrays, schema=self.schema))
        self._cols = [[] for _ in self.names]

    def close(self) -> None:
        self._flush()
        self._writer.close()


# --- the dump ---------------------------------------------------------------------------------


def _sql_member(z: zipfile.ZipFile, zip_path: Path) -> str:
    sql_files = [n for n in z.namelist() if n.lower().endswith(".sql")]
    if len(sql_files) != 1:
        raise DumpError(f"expected exactly one .sql file in {zip_path}, found {sql_files}")
    return sql_files[0]


def extract(dump_zip: Path, out_dir: Path, tables=TABLES) -> dict[str, int]:
    """Parse `tables` out of the dump into out_dir/<table>.parquet; returns {table: rows}."""
    dump_zip, out_dir = Path(dump_zip), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    wanted = {t: (f"INSERT INTO `{t}` VALUES ").encode() for t in tables}
    create_marks = {f"CREATE TABLE `{t}` ".encode(): t for t in tables}
    insert_prefixes = tuple(wanted.values())

    columns: dict[str, list[tuple[str, str]]] = {}
    writers: dict[str, TableWriter] = {}
    reading = None  # table whose CREATE TABLE block we are inside

    try:
        with zipfile.ZipFile(dump_zip) as z, z.open(_sql_member(z, dump_zip)) as src:
            for lineno, raw in enumerate(src, 1):
                if raw.startswith(b"CREATE TABLE "):
                    reading = next((t for mark, t in create_marks.items() if raw.startswith(mark)), None)
                    if reading:
                        columns[reading] = []
                    continue
                if reading:
                    line = raw.decode("utf-8")
                    m = _COLUMN.match(line)
                    if m:
                        columns[reading].append((m.group(1), m.group(2)))
                    elif line.startswith(")"):
                        reading = None
                    continue
                if not raw.startswith(insert_prefixes):
                    continue

                line = raw.decode("utf-8").rstrip("\r\n")
                table = line[len("INSERT INTO `") : line.index("`", 13)]
                if table not in columns:
                    raise DumpError(f"line {lineno}: rows of `{table}` come before its CREATE TABLE")
                writer = writers.get(table)
                if writer is None:
                    writer = writers[table] = TableWriter(table, columns[table], out_dir / f"{table}.parquet")
                for row in _parse_insert(line, len(wanted[table]), len(writer.names), f"line {lineno}"):
                    writer.add(row)
    except BaseException:
        for w in writers.values():  # do not leave half-written tables behind
            w.close()
            w.path.unlink(missing_ok=True)
        raise
    for w in writers.values():
        w.close()
    missing = [t for t in tables if t not in writers]
    if missing:
        raise DumpError(f"no rows found for table(s) {missing} in {dump_zip}")
    return {t: writers[t].rows for t in tables}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("dump", nargs="?", type=Path, default=DEFAULT_DUMP, help="the dump (a zip archive)")
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args(argv)
    if not args.dump.is_file():
        print(f"dump not found: {args.dump}", file=sys.stderr)
        return 1
    for table, rows in extract(args.dump, args.out_dir).items():
        print(f"wrote {rows} rows to {args.out_dir / (table + '.parquet')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
