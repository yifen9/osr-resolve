from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
from osr_pycore.utils.audit import Audit
from osr_pycore.utils.console import ConsoleSink
from osr_pycore.utils.jlog import jline
from osr_pycore.utils.logger import Logger
from osr_pycore.utils.meta import build_meta
from osr_pycore.utils.versioner import build_version_dir

from osr_resolve.tools.parquet_scan import read_schema, read_table
from osr_resolve.tools.parquet_write import write_parts
from osr_resolve.tools.stats_table import write_stats
from osr_resolve.tools.validate_columns import require_columns


@dataclass(frozen=True, slots=True)
class PlOrcid0010Out:
    run_dir: str
    meta: dict[str, Any]
    resolve_paths: list[str]
    map_paths: list[str]
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


def _build_resolve(table: pa.Table) -> pa.Table:
    xs_match_id = table.column("org_country_match_id").to_pylist()
    xs_match = table.column("org_country_match").to_pylist()
    xs_cnt = table.column("cnt").to_pylist()
    return pa.table(
        {
            "org_country_resolve_id": xs_match_id,
            "org_country_resolve": xs_match,
            "cnt": xs_cnt,
        }
    )


def _build_map(table: pa.Table) -> pa.Table:
    xs_match_id = table.column("org_country_match_id").to_pylist()
    return pa.table(
        {
            "org_country_match_id": xs_match_id,
            "org_country_resolve_id": xs_match_id,
        }
    )


def pl_orcid_0010_run(
    *,
    upstreams: list[str],
    input_dir: str,
    output_root: str,
    src: str,
    script_path: str,
    component: str,
    input_subdir_match: str,
    output_subdir_resolve: str,
    output_subdir_map: str,
    target_part_size_mb: int,
    batch_rows: int,
    compression: str,
    compression_level: int,
) -> PlOrcid0010Out:
    upstreams = _require_dirs(upstreams)
    input_dir = _require_dir(input_dir)
    output_root = os.path.abspath(output_root)
    src = _require_dir(src)
    script_path = _require_file(script_path)

    match_dir = _require_dir(_subdir(input_dir, input_subdir_match))

    schema = read_schema(match_dir)
    require_columns(schema, ["org_country_match_id", "org_country_match", "cnt"])

    repo_root = _repo_root_from_path(Path(script_path))
    env_path = _require_file(str(repo_root / "uv.lock"))

    params: dict[str, Any] = {
        "component": component,
        "upstreams": upstreams,
        "input_dir": input_dir,
        "output_root": output_root,
        "input_subdir_match": input_subdir_match,
        "output_subdir_resolve": output_subdir_resolve,
        "output_subdir_map": output_subdir_map,
        "target_part_size_mb": target_part_size_mb,
        "batch_rows": batch_rows,
        "compression": compression,
        "compression_level": compression_level,
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
        logger.info(jline("input", component, "match_dir", path=match_dir))

        match_table = read_table(match_dir)
        resolve_table = _build_resolve(match_table)
        map_table = _build_map(match_table)

        resolve_dir = _subdir(run_dir, output_subdir_resolve)
        map_dir = _subdir(run_dir, output_subdir_map)

        resolve_paths = write_parts(
            resolve_dir,
            resolve_table,
            target_part_size_mb=target_part_size_mb,
            compression=compression,
            compression_level=compression_level,
            batch_rows=batch_rows,
        )
        map_paths = write_parts(
            map_dir,
            map_table,
            target_part_size_mb=target_part_size_mb,
            compression=compression,
            compression_level=compression_level,
            batch_rows=batch_rows,
        )

        stats_path = write_stats(run_dir, logger=logger, component=component)

        logger.info(jline("output", component, "resolve_dir", path=resolve_dir))
        logger.info(jline("output", component, "map_dir", path=map_dir))
        logger.info(jline("output", component, "stats", path=stats_path))

        audit.finish_success()
        return PlOrcid0010Out(
            run_dir=run_dir,
            meta=meta,
            resolve_paths=resolve_paths,
            map_paths=map_paths,
            stats_path=stats_path,
        )
    except BaseException as e:
        audit.finish_error(e)
        raise
