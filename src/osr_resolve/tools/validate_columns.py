from __future__ import annotations

import pyarrow as pa


def require_columns(schema: pa.Schema, columns: list[str]) -> None:
    names = set(schema.names)
    missing = [x for x in columns if x not in names]
    if missing:
        raise ValueError(f"missing columns: {missing}")
