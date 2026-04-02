from __future__ import annotations

import argparse
from importlib import import_module
from pathlib import Path

from osr_pycore.io.yaml import read_yaml


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pl_geonames_0010_run")
    p.add_argument("src")
    p.add_argument("date")
    p.add_argument("upstream")
    p.add_argument("output_root")
    p.add_argument("input_root")
    p.add_argument("config")
    return p


def main(argv: list[str] | None = None) -> int:
    ns = _build_parser().parse_args(argv)
    cfg = read_yaml(ns.config)
    input_dir = str((Path(ns.input_root) / ns.date).resolve())
    mod = import_module("osr_resolve.pipeline.geonames.0010")
    out = mod.pl_geonames_0010_run(
        upstreams=[str(Path(ns.upstream).resolve()), input_dir],
        input_dir=input_dir,
        output_root=ns.output_root,
        src=ns.src,
        script_path=str(Path(__file__).resolve()),
        component=cfg["component"],
        input_file=cfg["input_file"],
        output_subdir_city=cfg["output_subdir_city"],
        target_part_size_mb=cfg["target_part_size_mb"],
        batch_rows=cfg["batch_rows"],
        compression=cfg["compression"],
        compression_level=cfg["compression_level"],
    )
    print(out.run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
