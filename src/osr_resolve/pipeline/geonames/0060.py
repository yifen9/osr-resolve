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

from osr_resolve.tools.parquet_scan import read_table
from osr_resolve.tools.parquet_write import write_parts
from osr_resolve.tools.stats_table import write_stats
from osr_resolve.tools.table_rule import apply_rules, append_const, append_null


@dataclass(frozen=True, slots=True)
class PlGeonames0060Out:
    run_dir: str
    meta: dict[str, Any]
    index_paths: list[str]
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


def _build_entries(
    city_table: pa.Table,
    alias_table: pa.Table,
    *,
    entry_rules_city: list[dict[str, Any]],
    entry_rules_alias: list[dict[str, Any]],
    value_source_kind_city: str,
    value_source_kind_alias: str,
) -> pa.Table:
    city_entry = apply_rules(city_table, entry_rules_city)
    city_entry = append_const(city_entry, output_col="source_kind", value=value_source_kind_city)
    if "alias_kind" not in city_entry.column_names:
        city_entry = append_null(city_entry, output_col="alias_kind")

    alias_entry = apply_rules(alias_table, entry_rules_alias)
    alias_entry = append_const(alias_entry, output_col="source_kind", value=value_source_kind_alias)
    if "country_code" not in alias_entry.column_names:
        alias_entry = append_null(alias_entry, output_col="country_code")

    return pa.concat_tables([city_entry, alias_entry], promote_options="default")


def _build_index(
    entries: pa.Table,
    *,
    column_index_norm: str,
    column_index_id: str,
    column_index_n_city: str,
    column_index_n_row: str,
    index_rules: list[dict[str, Any]],
) -> pa.Table:
    norms = entries.column(column_index_norm).to_pylist()
    city_ids = entries.column("city_id").to_pylist()

    grouped: dict[str, dict[str, Any]] = {}
    for norm, city_id in zip(norms, city_ids, strict=False):
        if norm is None or str(norm).strip() == "":
            continue
        group = grouped.get(norm)
        if group is None:
            grouped[norm] = {
                "cities": {city_id},
                "n_row": 1,
            }
        else:
            group["cities"].add(city_id)
            group["n_row"] += 1

    rows = []
    for norm in sorted(grouped.keys()):
        group = grouped[norm]
        rows.append(
            {
                column_index_norm: norm,
                column_index_n_city: len(group["cities"]),
                column_index_n_row: group["n_row"],
            }
        )

    table = pa.Table.from_pylist(rows)
    if table.num_rows == 0:
        table = pa.table(
            {
                column_index_norm: pa.array([], type=pa.string()),
                column_index_n_city: pa.array([], type=pa.int64()),
                column_index_n_row: pa.array([], type=pa.int64()),
            }
        )
    table = apply_rules(table, index_rules)
    if column_index_id not in table.column_names:
        raise ValueError(f"missing index id column after index_rules: {column_index_id}")
    return table


def _build_map(entries: pa.Table, *, map_rules: list[dict[str, Any]]) -> pa.Table:
    rows = entries.to_pylist()
    seen = set()
    uniq = []
    for row in rows:
        t = tuple(sorted(row.items()))
        if t in seen:
            continue
        seen.add(t)
        uniq.append(row)

    table = pa.Table.from_pylist(uniq)
    if table.num_rows == 0:
        table = pa.table(
            {
                "city_id": pa.array([], type=pa.string()),
                "city_key_norm": pa.array([], type=pa.string()),
                "country_code": pa.array([], type=pa.string()),
                "source_kind": pa.array([], type=pa.string()),
                "alias_kind": pa.array([], type=pa.string()),
            }
        )
    table = apply_rules(table, map_rules)
    return table


def pl_geonames_0060_run(
    *,
    upstreams: list[str],
    input_dir: str,
    output_root: str,
    src: str,
    script_path: str,
    component: str,
    input_subdir_city: str,
    input_subdir_alias: str,
    output_subdir_index: str,
    output_subdir_map: str,
    entry_rules_city: list[dict[str, Any]],
    entry_rules_alias: list[dict[str, Any]],
    index_rules: list[dict[str, Any]],
    map_rules: list[dict[str, Any]],
    column_index_norm: str,
    column_index_id: str,
    column_index_n_city: str,
    column_index_n_row: str,
    value_source_kind_city: str,
    value_source_kind_alias: str,
    target_part_size_mb: int,
    batch_rows: int,
    compression: str,
    compression_level: int,
) -> PlGeonames0060Out:
    upstreams = _require_dirs(upstreams)
    input_dir = _require_dir(input_dir)
    output_root = os.path.abspath(output_root)
    src = _require_dir(src)
    script_path = _require_file(script_path)

    city_in = _require_dir(_subdir(input_dir, input_subdir_city))
    alias_in = _require_dir(_subdir(input_dir, input_subdir_alias))

    repo_root = _repo_root_from_path(Path(script_path))
    env_path = _require_file(str(repo_root / "uv.lock"))

    params: dict[str, Any] = {
        "component": component,
        "upstreams": upstreams,
        "input_dir": input_dir,
        "output_root": output_root,
        "input_subdir_city": input_subdir_city,
        "input_subdir_alias": input_subdir_alias,
        "output_subdir_index": output_subdir_index,
        "output_subdir_map": output_subdir_map,
        "entry_rules_city": entry_rules_city,
        "entry_rules_alias": entry_rules_alias,
        "index_rules": index_rules,
        "map_rules": map_rules,
        "column_index_norm": column_index_norm,
        "column_index_id": column_index_id,
        "column_index_n_city": column_index_n_city,
        "column_index_n_row": column_index_n_row,
        "value_source_kind_city": value_source_kind_city,
        "value_source_kind_alias": value_source_kind_alias,
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
        logger.info(jline("input", component, "city_in", path=city_in))
        logger.info(jline("input", component, "alias_in", path=alias_in))

        city_table = read_table(city_in)
        alias_table = read_table(alias_in)

        entries = _build_entries(
            city_table,
            alias_table,
            entry_rules_city=entry_rules_city,
            entry_rules_alias=entry_rules_alias,
            value_source_kind_city=value_source_kind_city,
            value_source_kind_alias=value_source_kind_alias,
        )

        index_table = _build_index(
            entries,
            column_index_norm=column_index_norm,
            column_index_id=column_index_id,
            column_index_n_city=column_index_n_city,
            column_index_n_row=column_index_n_row,
            index_rules=index_rules,
        )

        map_table = _build_map(entries, map_rules=map_rules)

        index_out = _subdir(run_dir, output_subdir_index)
        map_out = _subdir(run_dir, output_subdir_map)

        index_paths = write_parts(
            index_out,
            index_table,
            target_part_size_mb=target_part_size_mb,
            compression=compression,
            compression_level=compression_level,
            batch_rows=batch_rows,
        )
        map_paths = write_parts(
            map_out,
            map_table,
            target_part_size_mb=target_part_size_mb,
            compression=compression,
            compression_level=compression_level,
            batch_rows=batch_rows,
        )

        stats_path = write_stats(run_dir, logger=logger, component=component)

        logger.info(jline("output", component, "index_out", path=index_out))
        logger.info(jline("output", component, "map_out", path=map_out))
        logger.info(jline("output", component, "stats", path=stats_path))

        audit.finish_success()
        return PlGeonames0060Out(
            run_dir=run_dir,
            meta=meta,
            index_paths=index_paths,
            map_paths=map_paths,
            stats_path=stats_path,
        )
    except BaseException as e:
        audit.finish_error(e)
        raise
