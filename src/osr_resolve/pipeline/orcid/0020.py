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


@dataclass(frozen=True, slots=True)
class PlOrcid0020Out:
    run_dir: str
    meta: dict[str, Any]
    match_paths: list[str]
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


def pl_orcid_0020_run(
    *,
    upstreams: list[str],
    input_dir_orcid: str,
    input_dir_geonames: str,
    output_root: str,
    src: str,
    script_path: str,
    component: str,
    input_subdir_orcid: str,
    input_subdir_index: str,
    input_subdir_map: str,
    output_subdir_match: str,
    output_subdir_map: str,
    column_orcid_match_id: str,
    column_orcid_norm: str,
    column_orcid_country: str,
    column_orcid_cnt: str,
    column_index_id: str,
    column_index_norm: str,
    column_map_index_id: str,
    column_map_city_id: str,
    column_map_country_code: str,
    column_map_source_kind: str,
    column_map_alias_kind: str,
    column_out_match_id: str,
    column_out_norm: str,
    column_out_country: str,
    column_out_key_id: str,
    column_out_n_candidate: str,
    column_out_n_candidate_country: str,
    column_out_status: str,
    column_out_map_match_id: str,
    column_out_map_city_id: str,
    column_out_map_key_id: str,
    column_out_map_country_code: str,
    column_out_map_source_kind: str,
    column_out_map_alias_kind: str,
    status_no_key: str,
    status_key_only: str,
    status_country_filtered_none: str,
    status_country_filtered_unique: str,
    status_country_filtered_multi: str,
    target_part_size_mb: int,
    batch_rows: int,
    compression: str,
    compression_level: int,
) -> PlOrcid0020Out:
    upstreams = _require_dirs(upstreams)
    input_dir_orcid = _require_dir(input_dir_orcid)
    input_dir_geonames = _require_dir(input_dir_geonames)
    output_root = os.path.abspath(output_root)
    src = _require_dir(src)
    script_path = _require_file(script_path)

    orcid_in = _require_dir(_subdir(input_dir_orcid, input_subdir_orcid))
    index_in = _require_dir(_subdir(input_dir_geonames, input_subdir_index))
    map_in = _require_dir(_subdir(input_dir_geonames, input_subdir_map))

    repo_root = _repo_root_from_path(Path(script_path))
    env_path = _require_file(str(repo_root / "uv.lock"))

    params: dict[str, Any] = {
        "component": component,
        "upstreams": upstreams,
        "input_dir_orcid": input_dir_orcid,
        "input_dir_geonames": input_dir_geonames,
        "output_root": output_root,
        "input_subdir_orcid": input_subdir_orcid,
        "input_subdir_index": input_subdir_index,
        "input_subdir_map": input_subdir_map,
        "output_subdir_match": output_subdir_match,
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
        logger.info(jline("input", component, "orcid_in", path=orcid_in))
        logger.info(jline("input", component, "index_in", path=index_in))
        logger.info(jline("input", component, "map_in", path=map_in))

        orcid_table = read_table(orcid_in)
        index_table = read_table(index_in)
        map_table = read_table(map_in)

        index_norm_to_key: dict[Any, Any] = {}
        for row in index_table.to_pylist():
            index_norm_to_key[row[column_index_norm]] = row[column_index_id]

        map_rows_by_key: dict[Any, list[dict[str, Any]]] = {}
        for row in map_table.to_pylist():
            key = row[column_map_index_id]
            map_rows_by_key.setdefault(key, []).append(row)

        match_rows: list[dict[str, Any]] = []
        map_rows_out: list[dict[str, Any]] = []

        for row in orcid_table.to_pylist():
            match_id = row[column_orcid_match_id]
            norm = row[column_orcid_norm]
            country = row[column_orcid_country]

            key_id = index_norm_to_key.get(norm)
            if key_id is None:
                match_rows.append(
                    {
                        column_out_match_id: match_id,
                        column_out_norm: norm,
                        column_out_country: country,
                        column_out_key_id: None,
                        column_out_n_candidate: 0,
                        column_out_n_candidate_country: 0,
                        column_out_status: status_no_key,
                    }
                )
                continue

            candidates = map_rows_by_key.get(key_id, [])
            n_candidate = len(candidates)

            country_filtered = []
            for cand in candidates:
                if cand.get(column_map_country_code) == country:
                    country_filtered.append(cand)

            n_candidate_country = len(country_filtered)

            if n_candidate_country == 0:
                status = status_country_filtered_none if n_candidate > 0 else status_no_key
            elif n_candidate_country == 1:
                status = status_country_filtered_unique
            else:
                status = status_country_filtered_multi

            if n_candidate > 0 and n_candidate_country == 0:
                status_key = status_key_only
            else:
                status_key = status

            match_rows.append(
                {
                    column_out_match_id: match_id,
                    column_out_norm: norm,
                    column_out_country: country,
                    column_out_key_id: key_id,
                    column_out_n_candidate: n_candidate,
                    column_out_n_candidate_country: n_candidate_country,
                    column_out_status: status_key,
                }
            )

            for cand in country_filtered:
                map_rows_out.append(
                    {
                        column_out_map_match_id: match_id,
                        column_out_map_city_id: cand[column_map_city_id],
                        column_out_map_key_id: key_id,
                        column_out_map_country_code: cand.get(column_map_country_code),
                        column_out_map_source_kind: cand.get(column_map_source_kind),
                        column_out_map_alias_kind: cand.get(column_map_alias_kind),
                    }
                )

        match_table = pa.Table.from_pylist(match_rows)
        map_table_out = pa.Table.from_pylist(map_rows_out)

        if match_table.num_rows == 0:
            match_table = pa.table(
                {
                    column_out_match_id: pa.array([], type=pa.string()),
                    column_out_norm: pa.array([], type=pa.string()),
                    column_out_country: pa.array([], type=pa.string()),
                    column_out_key_id: pa.array([], type=pa.string()),
                    column_out_n_candidate: pa.array([], type=pa.int64()),
                    column_out_n_candidate_country: pa.array([], type=pa.int64()),
                    column_out_status: pa.array([], type=pa.string()),
                }
            )

        if map_table_out.num_rows == 0:
            map_table_out = pa.table(
                {
                    column_out_map_match_id: pa.array([], type=pa.string()),
                    column_out_map_city_id: pa.array([], type=pa.string()),
                    column_out_map_key_id: pa.array([], type=pa.string()),
                    column_out_map_country_code: pa.array([], type=pa.string()),
                    column_out_map_source_kind: pa.array([], type=pa.string()),
                    column_out_map_alias_kind: pa.array([], type=pa.string()),
                }
            )

        match_out = _subdir(run_dir, output_subdir_match)
        map_out = _subdir(run_dir, output_subdir_map)

        match_paths = write_parts(
            match_out,
            match_table,
            target_part_size_mb=target_part_size_mb,
            compression=compression,
            compression_level=compression_level,
            batch_rows=batch_rows,
        )
        map_paths = write_parts(
            map_out,
            map_table_out,
            target_part_size_mb=target_part_size_mb,
            compression=compression,
            compression_level=compression_level,
            batch_rows=batch_rows,
        )

        stats_path = write_stats(run_dir, logger=logger, component=component)

        logger.info(jline("output", component, "match_out", path=match_out))
        logger.info(jline("output", component, "map_out", path=map_out))
        logger.info(jline("output", component, "stats", path=stats_path))

        audit.finish_success()
        return PlOrcid0020Out(
            run_dir=run_dir,
            meta=meta,
            match_paths=match_paths,
            map_paths=map_paths,
            stats_path=stats_path,
        )
    except BaseException as e:
        audit.finish_error(e)
        raise
