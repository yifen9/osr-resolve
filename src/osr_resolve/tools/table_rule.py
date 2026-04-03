from __future__ import annotations

import hashlib
import re
from typing import Any

import pyarrow as pa


def _norm_one(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip().lower()
    if not s:
        return None
    s = re.sub(r"[^0-9a-zA-Z]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    return s


def rename(table: pa.Table, rename_map: dict[str, str]) -> pa.Table:
    cols = []
    names = []
    for name in table.column_names:
        cols.append(table.column(name))
        names.append(rename_map.get(name, name))
    return pa.table(cols, names=names)


def keep(table: pa.Table, columns: list[str]) -> pa.Table:
    return table.select(columns)


def drop(table: pa.Table, columns: list[str]) -> pa.Table:
    xs = set(columns)
    ys = [name for name in table.column_names if name not in xs]
    return table.select(ys)


def append_norm(table: pa.Table, *, input_col: str, output_col: str) -> pa.Table:
    xs = table.column(input_col).to_pylist()
    ys = [_norm_one(x) for x in xs]
    arr = pa.array(ys, type=pa.string())
    if output_col in table.column_names:
        idx = table.column_names.index(output_col)
        return table.set_column(idx, output_col, arr)
    return table.append_column(output_col, arr)


def _hash_one(parts: list[Any], algo: str) -> str:
    s = "||".join("" if x is None else str(x) for x in parts)
    if algo == "md5":
        return hashlib.md5(s.encode("utf-8")).hexdigest()
    if algo == "sha1":
        return hashlib.sha1(s.encode("utf-8")).hexdigest()
    if algo == "sha256":
        return hashlib.sha256(s.encode("utf-8")).hexdigest()
    raise ValueError(f"unsupported algo: {algo}")


def append_hash(
    table: pa.Table, *, input_cols: list[str], output_col: str, algo: str
) -> pa.Table:
    cols = [table.column(name).to_pylist() for name in input_cols]
    rows = zip(*cols, strict=False)
    ys = [_hash_one(list(row), algo) for row in rows]
    arr = pa.array(ys, type=pa.string())
    if output_col in table.column_names:
        idx = table.column_names.index(output_col)
        return table.set_column(idx, output_col, arr)
    return table.append_column(output_col, arr)


def append_const(table: pa.Table, *, output_col: str, value: Any) -> pa.Table:
    if isinstance(value, str):
        arr = pa.array([value] * table.num_rows, type=pa.string())
    else:
        arr = pa.array([value] * table.num_rows)
    if output_col in table.column_names:
        idx = table.column_names.index(output_col)
        return table.set_column(idx, output_col, arr)
    return table.append_column(output_col, arr)


def append_null(table: pa.Table, *, output_col: str) -> pa.Table:
    arr = pa.array([None] * table.num_rows, type=pa.string())
    if output_col in table.column_names:
        idx = table.column_names.index(output_col)
        return table.set_column(idx, output_col, arr)
    return table.append_column(output_col, arr)


def apply_rules(table: pa.Table, rules: list[dict[str, Any]]) -> pa.Table:
    out = table
    for rule in rules:
        op = rule["op"]
        if op == "rename":
            out = rename(out, rule["map"])
            continue
        if op == "keep":
            out = keep(out, rule["columns"])
            continue
        if op == "drop":
            out = drop(out, rule["columns"])
            continue
        if op == "append_norm":
            out = append_norm(
                out,
                input_col=rule["input"],
                output_col=rule["output"],
            )
            continue
        if op == "append_hash":
            out = append_hash(
                out,
                input_cols=rule["inputs"],
                output_col=rule["output"],
                algo=rule["algo"],
            )
            continue
        if op == "append_const":
            out = append_const(
                out,
                output_col=rule["output"],
                value=rule["value"],
            )
            continue
        if op == "append_null":
            out = append_null(
                out,
                output_col=rule["output"],
            )
            continue
        raise ValueError(f"unsupported rule op: {op}")
    return out
