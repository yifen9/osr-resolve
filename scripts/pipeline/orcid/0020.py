from __future__ import annotations

import argparse
from importlib import import_module
from pathlib import Path

from osr_pycore.io.yaml import read_yaml
from osr_pycore.utils.versioner import index_dir, list_metas


def _require_last_dir(path: str) -> str:
    metas = list_metas(path)
    if not metas:
        raise FileNotFoundError(f"no version dirs under {path}")
    metas.sort(key=lambda x: x["timestamp"], reverse=True)
    d = index_dir(path, metas[0])
    if d is None:
        raise FileNotFoundError(f"cannot index last meta under {path}")
    return d


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pl_orcid_0020_run")
    p.add_argument("src")
    p.add_argument("date")
    p.add_argument("upstream_orcid")
    p.add_argument("upstream_geonames")
    p.add_argument("output_root")
    p.add_argument("input_root_orcid")
    p.add_argument("input_root_geonames")
    p.add_argument("config")
    return p


def main(argv: list[str] | None = None) -> int:
    ns = _build_parser().parse_args(argv)
    cfg = read_yaml(ns.config)

    input_stage_root_orcid = str((Path(ns.input_root_orcid) / ns.date / cfg["input_stage_root_orcid"]).resolve())
    input_dir_orcid = _require_last_dir(input_stage_root_orcid)
    input_dir_geonames = _require_last_dir(ns.input_root_geonames)

    mod = import_module("osr_resolve.pipeline.orcid.0020")
    out = mod.pl_orcid_0020_run(
        upstreams=[
            str(Path(ns.upstream_orcid).resolve()),
            str(Path(ns.upstream_geonames).resolve()),
            input_dir_orcid,
            input_dir_geonames,
        ],
        input_dir_orcid=input_dir_orcid,
        input_dir_geonames=input_dir_geonames,
        output_root=ns.output_root,
        src=ns.src,
        script_path=str(Path(__file__).resolve()),
        component=cfg["component"],
        input_subdir_orcid=cfg["input_subdir_orcid"],
        input_subdir_index=cfg["input_subdir_index"],
        input_subdir_map=cfg["input_subdir_map"],
        output_subdir_match=cfg["output_subdir_match"],
        output_subdir_map=cfg["output_subdir_map"],
        column_orcid_match_id=cfg["column_orcid_match_id"],
        column_orcid_norm=cfg["column_orcid_norm"],
        column_orcid_country=cfg["column_orcid_country"],
        column_orcid_cnt=cfg["column_orcid_cnt"],
        column_index_id=cfg["column_index_id"],
        column_index_norm=cfg["column_index_norm"],
        column_map_index_id=cfg["column_map_index_id"],
        column_map_city_id=cfg["column_map_city_id"],
        column_map_country_code=cfg["column_map_country_code"],
        column_map_source_kind=cfg["column_map_source_kind"],
        column_map_alias_kind=cfg["column_map_alias_kind"],
        column_out_match_id=cfg["column_out_match_id"],
        column_out_norm=cfg["column_out_norm"],
        column_out_country=cfg["column_out_country"],
        column_out_key_id=cfg["column_out_key_id"],
        column_out_n_candidate=cfg["column_out_n_candidate"],
        column_out_n_candidate_country=cfg["column_out_n_candidate_country"],
        column_out_status=cfg["column_out_status"],
        column_out_map_match_id=cfg["column_out_map_match_id"],
        column_out_map_city_id=cfg["column_out_map_city_id"],
        column_out_map_key_id=cfg["column_out_map_key_id"],
        column_out_map_country_code=cfg["column_out_map_country_code"],
        column_out_map_source_kind=cfg["column_out_map_source_kind"],
        column_out_map_alias_kind=cfg["column_out_map_alias_kind"],
        status_no_key=cfg["status_no_key"],
        status_key_only=cfg["status_key_only"],
        status_country_filtered_none=cfg["status_country_filtered_none"],
        status_country_filtered_unique=cfg["status_country_filtered_unique"],
        status_country_filtered_multi=cfg["status_country_filtered_multi"],
        target_part_size_mb=cfg["target_part_size_mb"],
        batch_rows=cfg["batch_rows"],
        compression=cfg["compression"],
        compression_level=cfg["compression_level"],
    )
    print(out.run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
