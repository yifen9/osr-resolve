from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
from osr_pycore.io.parquet import read_parquet
from osr_pycore.utils.audit import Audit
from osr_pycore.utils.console import ConsoleSink
from osr_pycore.utils.jlog import jline
from osr_pycore.utils.logger import Logger
from osr_pycore.utils.meta import build_meta
from osr_pycore.utils.versioner import build_version_dir

from osr_resolve.tools.parquet_scan import list_part_paths, read_schema
from osr_resolve.tools.parquet_write import write_part
from osr_resolve.tools.stats_table import write_stats
from osr_resolve.tools.validate_columns import require_columns


@dataclass(frozen=True, slots=True)
class PlGeonames0030Out:
    run_dir: str
    meta: dict[str, Any]
    city_paths: list[str]
    stats_path: str


def _require_file(path: str) -> str:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    return os.path.abspath(path)


def _require_dir(path: str) -> str:
    if not os.path.isdir(path):
        raise FileNotFoundError(path)
    return os.path.abspath(path)


def _require_dirs(paths: list[str]) -> list[str]:
    return [_require_dir(path) for path in paths]


def _repo_root_from_path(p: Path) -> Path:
    cur = p.resolve()
    if cur.is_file():
        cur = cur.parent
    for _ in range(16):
        if (cur / "uv.lock").is_file() and (cur / "pyproject.toml").is_file():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    raise FileNotFoundError("repo root not found (expected uv.lock and pyproject.toml)")


def _subdir(path: str, name: str) -> str:
    return os.path.join(path, name)


def _keep(table: pa.Table, keep_columns: list[str]) -> pa.Table:
    return table.select(keep_columns)


def pl_geonames_0030_run(
    *,
    upstreams: list[str],
    input_dir: str,
    output_root: str,
    src: str,
    script_path: str,
    component: str,
    input_subdir_city: str,
    output_subdir_city: str,
    keep_columns: list[str],
    compression: str,
    compression_level: int,
) -> PlGeonames0030Out:
    upstreams = _require_dirs(upstreams)
    input_dir = _require_dir(input_dir)
    output_root = os.path.abspath(output_root)
    src = _require_dir(src)
    script_path = _require_file(script_path)

    city_in = _require_dir(_subdir(input_dir, input_subdir_city))
    schema = read_schema(city_in)
    require_columns(schema, keep_columns)
    part_paths_in = list_part_paths(city_in)

    repo_root = _repo_root_from_path(Path(script_path))
    env_path = _require_file(str(repo_root / "uv.lock"))

    params: dict[str, Any] = {
        "component": component,
        "upstreams": upstreams,
        "input_dir": input_dir,
        "output_root": output_root,
        "input_subdir_city": input_subdir_city,
        "output_subdir_city": output_subdir_city,
        "keep_columns": keep_columns,
        "compression": compression,
        "compression_level": compression_level,
        "input_schema": [{"name": f.name, "type": str(f.type)} for f in schema],
        "n_input_parts": len(part_paths_in),
    }

    meta = build_meta(
        params=params,
        env=env_path,
        script=script_path,
        src=src,
    )

    run_dir = build_version_dir(output_root, meta)
    audit = Audit.create(run_dir, meta)
    logger = Logger(sinks=[ConsoleSink(), audit])

    try:
        logger.info(jline("stage", component, "start", run_dir=run_dir))
        for upstream in upstreams:
            logger.info(jline("input", component, "upstream", path=upstream))
        logger.info(jline("input", component, "input_dir", path=input_dir))
        logger.info(jline("input", component, "city_in", path=city_in))

        city_out = _subdir(run_dir, output_subdir_city)
        city_paths: list[str] = []

        for index, part_path_in in enumerate(part_paths_in):
            table_in = read_parquet(part_path_in)
            table_out = _keep(table_in, keep_columns)
            part_path_out = write_part(
                city_out,
                table_out,
                index=index,
                compression=compression,
                compression_level=compression_level,
            )
            city_paths.append(part_path_out)

        stats_path = write_stats(run_dir, logger=logger, component=component)

        logger.info(jline("output", component, "city_out", path=city_out))
        logger.info(jline("output", component, "stats", path=stats_path))

        audit.finish_success()
        return PlGeonames0030Out(
            run_dir=run_dir,
            meta=meta,
            city_paths=city_paths,
            stats_path=stats_path,
        )
    except BaseException as e:
        audit.finish_error(e)
        raise
