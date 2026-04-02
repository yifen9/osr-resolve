from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from osr_pycore.io.json import write_json


def _ensure_parent(path: str) -> str:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return os.path.abspath(path)


def write_json_file(path: str, obj: dict[str, Any] | list[Any]) -> str:
    path = _ensure_parent(path)
    write_json(path, obj)
    return path
