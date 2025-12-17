"""
Download Command CLI
====================

Command-line interface for downloading HMDA data files from CFPB.
"""

import argparse
import logging

from ..core.workflows import download_workflow

logger = logging.getLogger(__name__)


def configure_download_parser(parser: argparse.ArgumentParser) -> None:
    """
    Configure the download subcommand parser.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser to configure
    """
    parser.description = """
Download HMDA data files from the CFPB website.

Files are automatically organized into subdirectories:
- loans/              (LAR files)
- panel/              (Panel files)
- transmissal_series/ (TS files)
- msamd/              (Geography files)
- misc/               (Other files)

Examples:
  # Download data for 2018-2024
  hmda download --years 2018-2024

  # Download with MLAR files
  hmda download --years 2020-2024 --include-mlar

  # Download historical data
  hmda download --years 2018-2024 --include-historical

  # Download to custom location
  hmda download --years 2020-2024 --destination ./my_data
    """

    parser.add_argument(
        "--years",
        type=str,
        required=True,
        metavar="START-END",
        help="Year range to download (e.g., '2018-2024' or '2020' for single year)",
    )

    parser.add_argument(
        "--destination",
        type=str,
        default=None,
        metavar="PATH",
        help="Destination folder for downloads (default: ./data/raw)",
    )

    parser.add_argument(
        "--include-mlar",
        action="store_true",
        help="Include Modified LAR (MLAR) files",
    )

    parser.add_argument(
        "--include-historical",
        action="store_true",
        help="Include historical 2007-2017 files",
    )

    parser.add_argument(
        "--pause",
        type=int,
        default=5,
        metavar="SECONDS",
        help="Seconds to pause between downloads (default: 5)",
    )

    parser.add_argument(
        "--wait",
        type=int,
        default=10,
        metavar="SECONDS",
        help="Seconds to wait for JavaScript to load (default: 10)",
    )

    parser.add_argument(
        "--overwrite",
        choices=["skip", "always", "if_newer", "if_size_diff"],
        default="skip",
        help="Overwrite behavior for existing files (default: skip)",
    )

    parser.set_defaults(handler=handle_download_command)


def parse_year_range(year_str: str) -> range:
    """
    Parse year range string into a range object.

    Parameters
    ----------
    year_str : str
        Year range string (e.g., "2018-2024" or "2020")

    Returns
    -------
    range
        Range object representing the years

    Raises
    ------
    ValueError
        If the year string format is invalid
    """
    if "-" in year_str:
        parts = year_str.split("-")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid year range: {year_str}. "
                "Expected format: 'START-END' (e.g., '2018-2024')"
            )
        try:
            start_year = int(parts[0])
            end_year = int(parts[1])
        except ValueError:
            raise ValueError(
                f"Invalid year range: {year_str}. Years must be integers."
            )

        if start_year > end_year:
            raise ValueError(
                f"Invalid year range: {year_str}. Start year must be <= end year."
            )

        return range(start_year, end_year + 1)
    else:
        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(f"Invalid year: {year_str}. Expected integer or range.")

        return range(year, year + 1)


def handle_download_command(args: argparse.Namespace) -> int:
    """
    Handle the download command.

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
        # Parse year range
        years = parse_year_range(args.years)

        # Execute download workflow
        download_workflow(
            years=years,
            destination_folder=args.destination,
            include_mlar=args.include_mlar,
            include_historical=args.include_historical,
            pause_length=args.pause,
            wait_time=args.wait,
            overwrite_mode=args.overwrite,
        )

        return 0

    except ValueError as e:
        logger.error(f"Invalid argument: {e}")
        return 1
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return 1
