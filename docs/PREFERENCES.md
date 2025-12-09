# Coding Preferences

General principles extracted from this codebase.

## Architecture & Design

**Layered pipelines**: Use staged transformation architectures (e.g., raw → bronze → silver) where each layer has clear responsibilities and increasing refinement.

**Explicit over implicit**: Prefer clear, composable functions over monolithic convenience wrappers. Make workflows visible and understandable.

**Minimal abstraction**: Avoid premature generalization. Don't create helpers for one-time operations. Three similar lines are better than a forced abstraction.

**Progressive complexity**: Start simple, add sophistication later. Early stages preserve everything; later stages apply transformations and optimizations.

## Tools & Technology

**Modern tooling**: Prefer efficient, modern libraries (e.g., Polars over Pandas, pathlib.Path over string paths).

**Lazy evaluation**: Use streaming/lazy operations to avoid loading entire datasets into memory. Scan, don't read.

**Partitioning for performance**: Organize data for efficient querying (e.g., Hive partitioning by relevant dimensions).

## Code Quality

**Type hints and docstrings**: All public functions should have type annotations and clear docstrings.

**Structured logging**: Use proper logging with appropriate levels (DEBUG/INFO/WARNING/ERROR), not print statements.

**Consistent patterns**: Standardize function signatures and parameters across similar operations (e.g., `min_year`, `max_year`, `replace=False`).

**Simple return values**: Return None from procedures, log errors. Avoid complex status objects.

## Development Workflow

**Example-driven documentation**: Show how to use code through comprehensive, numbered examples (`01_`, `02_`, `99_`). Examples over API docs.

**Read before editing**: Understand existing implementation before making changes.

**Small, focused changes**: Avoid large refactors. Make incremental improvements.

**Delete, don't comment**: Remove unused code completely. No backwards-compatibility hacks or deprecation warnings.

**Configuration with defaults**: Use environment variables for paths/settings, but provide sensible defaults.

## Data Processing

**One file at a time**: Process inputs sequentially to ensure consistency. Avoid scanning entire directories when unnecessary.

**Fail gracefully**: Continue processing other items when one fails. Log errors, don't halt.

**Schema stability**: Maintain consistent data types across partitions and time periods.

**Testing as needed**: Write tests for critical functions and edge cases, not for everything. Pragmatic over dogmatic.