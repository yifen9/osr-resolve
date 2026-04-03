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
    p = argparse.ArgumentParser(prog="pl_geonames_0060_run")
    p.add_argument("src")
    p.add_argument("upstream")
    p.add_argument("output_root")
    p.add_argument("input_root")
    p.add_argument("config")
    return p


def main(argv: list[str] | None = None) -> int:
    ns = _build_parser().parse_args(argv)
    cfg = read_yaml(ns.config)
    input_dir = _require_last_dir(ns.input_root)
    mod = import_module("osr_resolve.pipeline.geonames.0060")
    out = mod.pl_geonames_0060_run(
        upstreams=[str(Path(ns.upstream).resolve()), input_dir],
        input_dir=input_dir,
        output_root=ns.output_root,
        src=ns.src,
        script_path=str(Path(__file__).resolve()),
        component=cfg["component"],
        input_subdir_city=cfg["input_subdir_city"],
        input_subdir_alias=cfg["input_subdir_alias"],
        output_subdir_index=cfg["output_subdir_index"],
        output_subdir_map=cfg["output_subdir_map"],
        entry_rules_city=cfg["entry_rules_city"],
        entry_rules_alias=cfg["entry_rules_alias"],
        index_rules=cfg["index_rules"],
        map_rules=cfg["map_rules"],
        column_index_norm=cfg["column_index_norm"],
        column_index_id=cfg["column_index_id"],
        column_index_n_city=cfg["column_index_n_city"],
        column_index_n_row=cfg["column_index_n_row"],
        value_source_kind_city=cfg["value_source_kind_city"],
        value_source_kind_alias=cfg["value_source_kind_alias"],
        target_part_size_mb=cfg["target_part_size_mb"],
        batch_rows=cfg["batch_rows"],
        compression=cfg["compression"],
        compression_level=cfg["compression_level"],
    )
    print(out.run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
