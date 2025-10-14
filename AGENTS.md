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

