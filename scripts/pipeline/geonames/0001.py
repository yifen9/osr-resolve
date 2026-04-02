from __future__ import annotations

import argparse
from pathlib import Path

from osr_pycore.io.yaml import read_yaml
from osr_pycore.tools.source_meta import create_source_meta_run


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pl_geonames_0001")
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
    upstreams = [str(Path(ns.upstream).resolve()), input_dir]
    out = create_source_meta_run(
        upstreams=upstreams,
        input_dir=input_dir,
        output_root=ns.output_root,
        src=ns.src,
        script_path=str(Path(__file__).resolve()),
        source_name=cfg["source_name"],
        component=cfg["component"],
    )
    print(out.run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
