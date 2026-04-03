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
    p = argparse.ArgumentParser(prog="pl_geonames_0050_run")
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
    mod = import_module("osr_resolve.pipeline.geonames.0050")
    out = mod.pl_geonames_0050_run(
        upstreams=[str(Path(ns.upstream).resolve()), input_dir],
        input_dir=input_dir,
        output_root=ns.output_root,
        src=ns.src,
        script_path=str(Path(__file__).resolve()),
        component=cfg["component"],
        input_subdir_city=cfg["input_subdir_city"],
        input_subdir_alias=cfg["input_subdir_alias"],
        output_subdir_city=cfg["output_subdir_city"],
        output_subdir_alias=cfg["output_subdir_alias"],
        rules_city=cfg["rules_city"],
        rules_alias=cfg["rules_alias"],
        compression=cfg["compression"],
        compression_level=cfg["compression_level"],
    )
    print(out.run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
