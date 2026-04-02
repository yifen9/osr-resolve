from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pyarrow as pa
from osr_pycore.utils.logger import Logger
from osr_pycore.utils.progress import Progress

from osr_resolve.tools.json_write import write_json_file
from osr_resolve.tools.parquet_scan import (
    count_parts,
    list_part_paths,
    read_schema,
    read_table,
)


def _require_dir(path: str) -> str:
    if not os.path.isdir(path):
        raise FileNotFoundError(path)
    return os.path.abspath(path)


def table_stats(table: pa.Table) -> dict[str, Any]:
    return {
        "n_row": table.num_rows,
        "n_col": table.num_columns,
        "columns": list(table.column_names),
    }


def dir_stats(path: str) -> dict[str, Any]:
    path = _require_dir(path)
    part_paths = list_part_paths(path)
    table = read_table(path)
    schema = read_schema(path)
    return {
        "path": path,
        "n_part": count_parts(path),
        "part_files": [Path(x).name for x in part_paths],
        "schema": [{"name": f.name, "type": str(f.type)} for f in schema],
        "table": table_stats(table),
    }


def _list_stat_dirs(path: str) -> list[Path]:
    path = _require_dir(path)
    xs: list[Path] = []
    for p in sorted(Path(path).iterdir()):
        if not p.is_dir():
            continue
        if p.name.startswith("_"):
            continue
        if not list_part_paths(str(p)):
            continue
        xs.append(p)
    return xs


def run_stats(
    path: str, *, logger: Logger | None = None, component: str | None = None
) -> dict[str, Any]:
    path = _require_dir(path)
    dirs = _list_stat_dirs(path)
    out: dict[str, Any] = {}

    progress: Progress | None = None
    if logger is not None:
        progress = Progress(
            logger=logger,
            name=component or "stats",
            total=len(dirs),
        )
        progress.start()

    try:
        for p in dirs:
            out[p.name] = dir_stats(str(p))
            if progress is not None:
                progress.step(1)
    finally:
        if progress is not None:
            progress.finish()

    return out


def write_stats(
    path: str, *, logger: Logger | None = None, component: str | None = None
) -> str:
    path = _require_dir(path)
    out_path = os.path.join(path, "_stats.json")
    write_json_file(out_path, run_stats(path, logger=logger, component=component))
    return out_path
