from __future__ import annotations

import argparse
import hashlib
import os
from typing import Final


_BUF_SIZE: Final[int] = 1024 * 1024


def _require_file(path: str) -> str:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    return os.path.abspath(path)


def _digest(path: str, name: str) -> str:
    path = _require_file(path)
    h = hashlib.new(name)
    with open(path, "rb") as f:
        while True:
            chunk = f.read(_BUF_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def md5_file(path: str) -> str:
    return _digest(path, "md5")


def sha1_file(path: str) -> str:
    return _digest(path, "sha1")


def sha256_file(path: str) -> str:
    return _digest(path, "sha256")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hash_file")
    p.add_argument("algo")
    p.add_argument("path")
    return p


def main(argv: list[str] | None = None) -> int:
    ns = _build_parser().parse_args(argv)
    algo = ns.algo.lower()
    if algo == "md5":
        print(md5_file(ns.path))
        return 0
    if algo == "sha1":
        print(sha1_file(ns.path))
        return 0
    if algo == "sha256":
        print(sha256_file(ns.path))
        return 0
    raise ValueError(f"unsupported algo: {ns.algo}")


if __name__ == "__main__":
    raise SystemExit(main())
