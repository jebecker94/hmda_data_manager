"""
Import Command CLI
==================

Command-line interface for importing and processing HMDA data.
"""

import argparse
import logging

from ..core.workflows import (
    import_post2018_workflow,
    import_2007_2017_workflow,
    import_pre2007_workflow,
)

logger = logging.getLogger(__name__)


def configure_import_parser(parser: argparse.ArgumentParser) -> None:
    """
    Configure the import subcommand parser.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser to configure
    """
    parser.description = """
Import and process HMDA data (bronze + silver layers).

This command builds both bronze and silver layers for HMDA data across
three different time periods:

- post2018: Post-2018 data (2018-2024) with expanded schema
- 2007-2017: Historical data (2007-2017) with standardized format
- pre2007: Historical data (1990-2006) from openICPSR

Examples:
  # Import post-2018 data
  hmda import post2018 --min-year 2018 --max-year 2024

  # Import 2007-2017 data (without tract variables)
  hmda import 2007-2017 --min-year 2007 --max-year 2017 --drop-tract-vars

  # Import pre-2007 data
  hmda import pre2007 --min-year 1990 --max-year 2006

  # Import only specific datasets (post-2018 only)
  hmda import post2018 --min-year 2020 --max-year 2024 --datasets loans panel

  # Replace existing files
  hmda import post2018 --min-year 2018 --max-year 2024 --replace
    """

    # Create subparsers for different time periods
    subparsers = parser.add_subparsers(
        dest="period",
        required=True,
        help="Time period to process",
    )

    # Post-2018 parser
    post2018_parser = subparsers.add_parser(
        "post2018",
        help="Import post-2018 data (2018-2024)",
    )
    post2018_parser.add_argument(
        "--min-year",
        type=int,
        default=2018,
        help="Minimum year to process (default: 2018)",
    )
    post2018_parser.add_argument(
        "--max-year",
        type=int,
        default=2024,
        help="Maximum year to process (default: 2024)",
    )
    post2018_parser.add_argument(
        "--datasets",
        nargs="+",
        choices=["loans", "panel", "transmissal_series"],
        default=None,
        help="Datasets to process (default: all three)",
    )
    post2018_parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing files (default: skip existing)",
    )
    post2018_parser.set_defaults(handler=handle_post2018_command)

    # 2007-2017 parser
    period_2007_2017_parser = subparsers.add_parser(
        "2007-2017",
        help="Import 2007-2017 data",
    )
    period_2007_2017_parser.add_argument(
        "--min-year",
        type=int,
        default=2007,
        help="Minimum year to process (default: 2007)",
    )
    period_2007_2017_parser.add_argument(
        "--max-year",
        type=int,
        default=2017,
        help="Maximum year to process (default: 2017)",
    )
    period_2007_2017_parser.add_argument(
        "--drop-tract-vars",
        action="store_true",
        help="Drop bulky census tract variables (recommended)",
    )
    period_2007_2017_parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing files (default: skip existing)",
    )
    period_2007_2017_parser.set_defaults(handler=handle_2007_2017_command)

    # Pre-2007 parser
    pre2007_parser = subparsers.add_parser(
        "pre2007",
        help="Import pre-2007 data (1990-2006)",
    )
    pre2007_parser.add_argument(
        "--min-year",
        type=int,
        default=1990,
        help="Minimum year to process (default: 1990)",
    )
    pre2007_parser.add_argument(
        "--max-year",
        type=int,
        default=2006,
        help="Maximum year to process (default: 2006)",
    )
    pre2007_parser.add_argument(
        "--datasets",
        nargs="+",
        choices=["loans", "panel", "transmissal_series"],
        default=None,
        help="Datasets to process (default: all three)",
    )
    pre2007_parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing files (default: skip existing)",
    )
    pre2007_parser.set_defaults(handler=handle_pre2007_command)


def handle_import_command(args: argparse.Namespace) -> int:
    """
    Handle the import command by dispatching to period-specific handlers.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command line arguments

    Returns
    -------
    int
        Exit code (0 for success, non-zero for failure)
    """
    if not hasattr(args, "handler"):
        logger.error("No handler configured for import command")
        return 1

    return args.handler(args)


def handle_post2018_command(args: argparse.Namespace) -> int:
    """
    Handle the post-2018 import command.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command line arguments

    Returns
    -------
    int
        Exit code (0 for success, non-zero for failure)
    """
    try:
        results = import_post2018_workflow(
            min_year=args.min_year,
            max_year=args.max_year,
            datasets=args.datasets,
            replace=args.replace,
        )

        # Check if any datasets failed
        if not all(results.values()):
            logger.warning("Some datasets failed to process")
            return 1

        return 0

    except Exception as e:
        logger.error(f"Post-2018 import failed: {e}")
        return 1


def handle_2007_2017_command(args: argparse.Namespace) -> int:
    """
    Handle the 2007-2017 import command.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command line arguments

    Returns
    -------
    int
        Exit code (0 for success, non-zero for failure)
    """
    try:
        success = import_2007_2017_workflow(
            min_year=args.min_year,
            max_year=args.max_year,
            drop_tract_vars=args.drop_tract_vars,
            replace=args.replace,
        )

        return 0 if success else 1

    except Exception as e:
        logger.error(f"2007-2017 import failed: {e}")
        return 1


def handle_pre2007_command(args: argparse.Namespace) -> int:
    """
    Handle the pre-2007 import command.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command line arguments

    Returns
    -------
    int
        Exit code (0 for success, non-zero for failure)
    """
    try:
        results = import_pre2007_workflow(
            min_year=args.min_year,
            max_year=args.max_year,
            datasets=args.datasets,
            replace=args.replace,
        )

        # Check if any datasets failed
        if not all(results.values()):
            logger.warning("Some datasets failed to process")
            return 1

        return 0

    except Exception as e:
        logger.error(f"Pre-2007 import failed: {e}")
        return 1
