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
from osr_resolve.tools.parquet_write import write_part, write_parts
from osr_resolve.tools.stats_table import write_stats
from osr_resolve.tools.validate_columns import require_columns


@dataclass(frozen=True, slots=True)
class PlGeonames0040Out:
    run_dir: str
    meta: dict[str, Any]
    city_paths: list[str]
    alias_paths: list[str]
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


def _norm_text(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    return s


def _split_alt(x: Any) -> list[str]:
    s = _norm_text(x)
    if s is None:
        return []
    xs = []
    for part in s.split(","):
        y = _norm_text(part)
        if y is not None:
            xs.append(y)
    return xs


def _build_alias(
    table: pa.Table,
    *,
    column_city_id: str,
    column_name: str,
    column_ascii: str,
    column_alt: str,
) -> pa.Table:
    city_ids = table.column(column_city_id).to_pylist()
    names = table.column(column_name).to_pylist()
    asciis = table.column(column_ascii).to_pylist()
    alts = table.column(column_alt).to_pylist()

    xs_city_id: list[Any] = []
    xs_alias: list[str] = []
    xs_kind: list[str] = []

    seen: set[tuple[Any, str, str]] = set()

    for city_id, name, ascii_name, alt_names in zip(
        city_ids, names, asciis, alts, strict=False
    ):
        v_name = _norm_text(name)
        if v_name is not None:
            key = (city_id, v_name, "name")
            if key not in seen:
                seen.add(key)
                xs_city_id.append(city_id)
                xs_alias.append(v_name)
                xs_kind.append("name")

        v_ascii = _norm_text(ascii_name)
        if v_ascii is not None:
            key = (city_id, v_ascii, "ascii")
            if key not in seen:
                seen.add(key)
                xs_city_id.append(city_id)
                xs_alias.append(v_ascii)
                xs_kind.append("ascii")

        for v_alt in _split_alt(alt_names):
            key = (city_id, v_alt, "alt")
            if key not in seen:
                seen.add(key)
                xs_city_id.append(city_id)
                xs_alias.append(v_alt)
                xs_kind.append("alt")

    return pa.table(
        {
            "city_id": xs_city_id,
            "city_alias": xs_alias,
            "alias_kind": xs_kind,
        }
    )


def _drop_city_columns(table: pa.Table, drop_columns_city: list[str]) -> pa.Table:
    keep = [name for name in table.column_names if name not in set(drop_columns_city)]
    return table.select(keep)


def pl_geonames_0040_run(
    *,
    upstreams: list[str],
    input_dir: str,
    output_root: str,
    src: str,
    script_path: str,
    component: str,
    input_subdir_city: str,
    output_subdir_city: str,
    output_subdir_alias: str,
    column_city_id: str,
    column_name: str,
    column_ascii: str,
    column_alt: str,
    drop_columns_city: list[str],
    target_part_size_mb_alias: int,
    batch_rows_alias: int,
    compression: str,
    compression_level: int,
) -> PlGeonames0040Out:
    upstreams = _require_dirs(upstreams)
    input_dir = _require_dir(input_dir)
    output_root = os.path.abspath(output_root)
    src = _require_dir(src)
    script_path = _require_file(script_path)

    city_in = _require_dir(_subdir(input_dir, input_subdir_city))
    schema = read_schema(city_in)
    require_columns(schema, [column_city_id, column_name, column_ascii, column_alt])
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
        "output_subdir_alias": output_subdir_alias,
        "column_city_id": column_city_id,
        "column_name": column_name,
        "column_ascii": column_ascii,
        "column_alt": column_alt,
        "drop_columns_city": drop_columns_city,
        "target_part_size_mb_alias": target_part_size_mb_alias,
        "batch_rows_alias": batch_rows_alias,
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
        alias_out = _subdir(run_dir, output_subdir_alias)

        city_paths: list[str] = []
        alias_tables: list[pa.Table] = []

        for index, part_path_in in enumerate(part_paths_in):
            table_city_in = read_parquet(part_path_in)
            table_alias = _build_alias(
                table_city_in,
                column_city_id=column_city_id,
                column_name=column_name,
                column_ascii=column_ascii,
                column_alt=column_alt,
            )
            table_city_out = _drop_city_columns(table_city_in, drop_columns_city)

            city_path = write_part(
                city_out,
                table_city_out,
                index=index,
                compression=compression,
                compression_level=compression_level,
            )

            city_paths.append(city_path)
            alias_tables.append(table_alias)

        table_alias_all = (
            pa.concat_tables(alias_tables, promote_options="default")
            if alias_tables
            else pa.table(
                {
                    "city_id": [],
                    "city_alias": [],
                    "alias_kind": [],
                }
            )
        )

        alias_paths = write_parts(
            alias_out,
            table_alias_all,
            target_part_size_mb=target_part_size_mb_alias,
            compression=compression,
            compression_level=compression_level,
            batch_rows=batch_rows_alias,
        )

        stats_path = write_stats(run_dir, logger=logger, component=component)

        logger.info(jline("output", component, "city_out", path=city_out))
        logger.info(jline("output", component, "alias_out", path=alias_out))
        logger.info(jline("output", component, "stats", path=stats_path))

        audit.finish_success()
        return PlGeonames0040Out(
            run_dir=run_dir,
            meta=meta,
            city_paths=city_paths,
            alias_paths=alias_paths,
            stats_path=stats_path,
        )
    except BaseException as e:
        audit.finish_error(e)
        raise
