"""
HMDA Data Manager CLI
=====================

Command-line interface for HMDA data management workflows.

This module provides CLI commands for downloading and importing HMDA data.

Commands
--------
- hmda download: Download HMDA data files from CFPB
- hmda import: Import and process HMDA data (bronze + silver layers)

Example Usage
-------------
# Download data for 2018-2024
$ hmda download --years 2018-2024

# Import post-2018 data
$ hmda import post2018 --min-year 2018 --max-year 2024

# Import 2007-2017 data
$ hmda import 2007-2017 --min-year 2007 --max-year 2017 --drop-tract-vars

# Import pre-2007 data
$ hmda import pre2007 --min-year 1990 --max-year 2006

For detailed help on each command:
$ hmda download --help
$ hmda import --help
"""

import argparse
import logging
import sys
from typing import Sequence

from .download import configure_download_parser, handle_download_command
from .import_data import configure_import_parser, handle_import_command


def main(argv: Sequence[str] | None = None) -> int:
    """
    Main entry point for the HMDA CLI.

    Parameters
    ----------
    argv : Sequence[str] | None, optional
        Command line arguments. If None, uses sys.argv[1:]

    Returns
    -------
    int
        Exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(
        prog="hmda",
        description="HMDA Data Manager - Download and process HMDA data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download data for recent years
  hmda download --years 2018-2024

  # Download with MLAR files
  hmda download --years 2020-2024 --include-mlar

  # Import post-2018 data
  hmda import post2018 --min-year 2018 --max-year 2024

  # Import 2007-2017 data (without tract variables)
  hmda import 2007-2017 --min-year 2007 --max-year 2017 --drop-tract-vars

  # Import pre-2007 data
  hmda import pre2007 --min-year 1990 --max-year 2006

For more information, visit: https://github.com/yourusername/hmda_data_manager
        """,
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: INFO)",
    )

    # Create subcommands
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        help="Command to execute",
    )

    # Configure download subcommand
    download_parser = subparsers.add_parser(
        "download",
        help="Download HMDA data files from CFPB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    configure_download_parser(download_parser)

    # Configure import subcommand
    import_parser = subparsers.add_parser(
        "import",
        help="Import and process HMDA data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    configure_import_parser(import_parser)

    # Parse arguments
    args = parser.parse_args(argv)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Execute command
    try:
        if args.command == "download":
            return handle_download_command(args)
        elif args.command == "import":
            return handle_import_command(args)
        else:
            parser.print_help()
            return 1
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user", file=sys.stderr)
        return 130
    except Exception as e:
        logging.error(f"Command failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
