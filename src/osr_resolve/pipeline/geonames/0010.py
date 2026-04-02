from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from osr_pycore.io.parquet import read_parquet
from osr_pycore.utils.audit import Audit
from osr_pycore.utils.console import ConsoleSink
from osr_pycore.utils.jlog import jline
from osr_pycore.utils.logger import Logger
from osr_pycore.utils.meta import build_meta
from osr_pycore.utils.versioner import build_version_dir

from osr_resolve.tools.parquet_write import write_parts
from osr_resolve.tools.stats_table import write_stats


@dataclass(frozen=True, slots=True)
class PlGeonames0010Out:
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


def pl_geonames_0010_run(
    *,
    upstreams: list[str],
    input_dir: str,
    output_root: str,
    src: str,
    script_path: str,
    component: str,
    input_file: str,
    output_subdir_city: str,
    target_part_size_mb: int,
    batch_rows: int,
    compression: str,
    compression_level: int,
) -> PlGeonames0010Out:
    upstreams = _require_dirs(upstreams)
    input_dir = _require_dir(input_dir)
    output_root = os.path.abspath(output_root)
    src = _require_dir(src)
    script_path = _require_file(script_path)

    input_path = _require_file(os.path.join(input_dir, input_file))

    repo_root = _repo_root_from_path(Path(script_path))
    env_path = _require_file(str(repo_root / "uv.lock"))

    params: dict[str, Any] = {
        "component": component,
        "upstreams": upstreams,
        "input_dir": input_dir,
        "output_root": output_root,
        "input_file": input_file,
        "output_subdir_city": output_subdir_city,
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
        logger.info(jline("input", component, "input_file", path=input_path))

        table = read_parquet(input_path)
        city_dir = os.path.join(run_dir, output_subdir_city)
        city_paths = write_parts(
            city_dir,
            table,
            target_part_size_mb=target_part_size_mb,
            compression=compression,
            compression_level=compression_level,
            batch_rows=batch_rows,
        )

        stats_path = write_stats(run_dir, logger=logger, component=component)

        logger.info(jline("output", component, "city_dir", path=city_dir))
        logger.info(jline("output", component, "stats", path=stats_path))

        audit.finish_success()
        return PlGeonames0010Out(
            run_dir=run_dir,
            meta=meta,
            city_paths=city_paths,
            stats_path=stats_path,
        )
    except BaseException as e:
        audit.finish_error(e)
        raise
