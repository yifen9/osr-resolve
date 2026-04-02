from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
from osr_pycore.io.parquet import read_parquet


def _require_dir(path: str) -> str:
    if not os.path.isdir(path):
        raise FileNotFoundError(path)
    return os.path.abspath(path)


def list_part_paths(path: str) -> list[str]:
    path = _require_dir(path)
    xs = []
    for p in sorted(Path(path).glob("*.parquet")):
        if p.is_file():
            xs.append(str(p.resolve()))
    return xs


def read_part_tables(path: str) -> list[pa.Table]:
    xs = []
    for p in list_part_paths(path):
        xs.append(read_parquet(p))
    return xs


def read_table(path: str) -> pa.Table:
    xs = read_part_tables(path)
    if not xs:
        raise FileNotFoundError(f"no parquet parts under {path}")
    return pa.concat_tables(xs, promote_options="default")


def read_schema(path: str) -> pa.Schema:
    xs = read_part_tables(path)
    if not xs:
        raise FileNotFoundError(f"no parquet parts under {path}")
    return xs[0].schema


def count_parts(path: str) -> int:
    return len(list_part_paths(path))
