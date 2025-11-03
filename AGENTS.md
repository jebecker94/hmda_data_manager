# AGENTS Instructions

## Scope
These guidelines apply to the entire repository.

## Coding standards
- Target **Python 3.13**.
- Use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:
  - Format code with `ruff format <path>`.
  - Lint code with `ruff check <path>`.
- Favor readable, modular code with type hints and descriptive docstrings.
- Use `snake_case` for file names, modules and functions.

## Testing
- Run unit tests with `pytest` before committing.  If no tests are collected, note that in the PR description.
- Ensure new features include corresponding tests where practical.

## Commit guidelines
- Keep commit messages concise (<=72 characters for the summary).
- Reference related issues or context in the commit body when helpful.

## Data handling
- Do not commit large data files.  Paths and credentials should be provided through `config.py` or environment variables.

## Planning checklist
- Update `planning.md` as plans evolve.
- Check off items in `planning.md` as they are completed.

## Agent preferences and conventions

### General architecture
- Prefer a medallion layout: raw → bronze → silver (gold later).
- Directory layout: `data/{bronze,silver}/{loans,panel,transmissal_series}/{pre2007,period_2007_2017,post2018}`.
- Silver is hive-partitioned by `activity_year` and `file_type`; bronze is plain parquet.

### Polars-first data work
- Use Polars for new functionality; avoid pandas except where explicitly needed (e.g., export).
- Loading/writing:
  - Prefer `pl.scan_parquet(folder)` to load partitioned data.
  - For silver writes use `lf.sink_parquet(pl.PartitionByKey(out_dir, by=[pl.col("activity_year"), pl.col("file_type")], include_key=True))`.
  - Process one input file at a time in silver; ensure deterministic dtypes across files/years.
- Keep derived/duplicate fields minimal in bronze; do conversions in silver.

### Utilities organization
- Split utilities by concern and re-export via `utils/__init__.py`:
  - `utils/io.py`: delimiter sniffing, unzip, CSV header fixes.
  - `utils/schema.py`: HTML schema parsing, column renames.
  - `utils/cleaning.py`: NA handling, coercions, schema standardization, tract harmonization, plausibility, outliers, pipeline.
  - `utils/identity.py`: record keys, de-duplication.
  - `utils/geo.py`: tract variable splitting.
  - `utils/export.py`: Stata export.
- When moving functions, preserve logic; update imports and re-exports to keep the public API stable.

### Import pipeline conventions
- Post-2018: prefer explicit `build_bronze_post2018` and `build_silver_post2018` entry points over monolithic helpers.
- Avoid catch-all “convenience” functions (e.g., `import_hmda_data`); keep workflows explicit and composable.
- Put lender merge utilities under `core/lenders/` (not `utils`).

### Naming and terminology
- Python 3.13, Ruff format/lint, type hints, `snake_case` for files/functions.
- Use `period_2007_2017` (not `pre2018`) for 2007–2017 code.
- Examples: `01_`, `02_` are core workflow; non-core examples use the `99_` prefix.

### Deprecation policy
- Keep `deprecated/` as reference only; do not import from it.
- Before removing code, confirm no references remain (search and update imports).

### Docs and planning
- Keep planning split: `PLANNING.md` (core technical roadmap) vs `EXTENSIONS.md` (analysis/visualization/integrations).
- Update `README.md` with medallion layout and silver loading examples when relevant changes land.

### Implementation preferences
- Small, focused edits; avoid behavior changes unless requested.
- Maintain stable schemas and dtypes across partitions/years.
- Favor lazy/lower-memory operations and avoid scanning entire folders when single-file processing suffices.

### Testing and CI
- Run `ruff format` and `ruff check` locally.
- Use `pytest` for unit/integration tests; note in PR if no tests are collected.

