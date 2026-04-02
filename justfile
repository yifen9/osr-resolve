set shell := ["bash", "-lc"]

default:
    just --list

init:
    just venv && \
    just sync

venv:
    test -d .venv || uv venv

sync:
    uv sync --all-packages

sync-lock:
    uv sync --locked --all-packages

up:
    uv lock --upgrade

add PKG:
    uv add {{PKG}}

add-dev PKG:
    uv add --dev {{PKG}}

rm PKG:
    uv remove {{PKG}}

rm-dev PKG:
    uv remove --dev {{PKG}}

fmt:
    just sync && \
    uv run ruff format .

fmt-check:
    uv run ruff format --check .

lint:
    uv run ruff check . --fix

lint-check:
    uv run ruff check .

test:
    uv run pytest

ci:
    just venv && \
    just sync-lock && \
    just fmt-check && \
    just lint-check # && \
    # just test

docs-build:
	uv run pdoc --math -o site osr_resolve

docs-serve:
    uv run pdoc --math -p 8080 osr_resolve

find-last D:
    uv run python src/osr_resolve/tools/find_last.py {{D}}

SCRIPTS_ORCID_DATE := "2025_12_17"

pl-orcid-0001 S="src" D=SCRIPTS_ORCID_DATE U="data/pipeline/orcid/0000_raw" O="data/pipeline/orcid/0001_meta" I="data/external/orcid" C="config/pipeline/orcid/0001.yaml":
    uv run python scripts/pipeline/orcid/0001.py {{S}} {{D}} "$(just find-last {{U}})" {{O}} {{I}} {{C}}

pl-orcid-0001-s S="src" D=SCRIPTS_ORCID_DATE U="samples/pipeline/orcid/65536_0/0000_raw" O="samples/pipeline/orcid/65536_0/0001_meta" I="data/external/orcid" C="config/pipeline/orcid/0001.yaml":
    uv run python scripts/pipeline/orcid/0001.py {{S}} {{D}} "$(just find-last {{U}})" {{O}} {{I}} {{C}}
