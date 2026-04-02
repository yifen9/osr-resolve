from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
from osr_pycore.io.parquet import write_parquet


def _ensure_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return os.path.abspath(path)


def part_name(index: int) -> str:
    if index < 0:
        raise ValueError("part index must be >= 0")
    return f"{index:08d}.parquet"


def write_part(
    path: str,
    table: pa.Table,
    *,
    index: int,
    compression: str,
    compression_level: int | None,
) -> str:
    path = _ensure_dir(path)
    out = os.path.join(path, part_name(index))
    write_parquet(
        out,
        table,
        compression=compression,
        compression_level=compression_level,
    )
    return out


def write_parts(
    path: str,
    table: pa.Table,
    *,
    target_part_size_mb: int,
    compression: str,
    compression_level: int | None,
    batch_rows: int,
) -> list[str]:
    if target_part_size_mb <= 0:
        raise ValueError("target_part_size_mb must be > 0")
    if batch_rows <= 0:
        raise ValueError("batch_rows must be > 0")

    path = _ensure_dir(path)
    target_bytes = target_part_size_mb * 1024 * 1024
    batches = table.to_batches(max_chunksize=batch_rows)

    if not batches:
        out = write_part(
            path,
            table,
            index=0,
            compression=compression,
            compression_level=compression_level,
        )
        return [out]

    xs: list[str] = []
    cur: list[pa.RecordBatch] = []
    cur_bytes = 0
    part_index = 0

    for batch in batches:
        batch_bytes = batch.nbytes
        if cur and cur_bytes + batch_bytes > target_bytes:
            out = write_part(
                path,
                pa.Table.from_batches(cur),
                index=part_index,
                compression=compression,
                compression_level=compression_level,
            )
            xs.append(out)
            part_index += 1
            cur = []
            cur_bytes = 0
        cur.append(batch)
        cur_bytes += batch_bytes

    if cur:
        out = write_part(
            path,
            pa.Table.from_batches(cur),
            index=part_index,
            compression=compression,
            compression_level=compression_level,
        )
        xs.append(out)

    return xs
