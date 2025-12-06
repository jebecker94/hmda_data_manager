"""
Input/Output utilities for HMDA data (delimiters, CSV header tweaks, unzip).
"""

import io
import logging
import subprocess
import zipfile
from csv import Sniffer
from pathlib import Path


logger = logging.getLogger(__name__)


def normalized_file_stem(stem: str) -> str:
    """Remove common suffixes from extracted archive names.

    Parameters
    ----------
    stem : str
        File stem (name without extension)

    Returns
    -------
    str
        Normalized file stem with common suffixes removed
    """
    if stem.endswith("_csv"):
        stem = stem[:-4]
    if stem.endswith("_pipe"):
        stem = stem[:-5]
    return stem


def should_process_output(path: Path, replace: bool) -> bool:
    """Return True when the target path should be generated.

    Parameters
    ----------
    path : Path
        Target output file path
    replace : bool
        Whether to replace existing files

    Returns
    -------
    bool
        True if file should be processed
    """
    return replace or not path.exists()


def get_delimiter(file_path: Path | str, bytes: int = 4096) -> str:
    """Determine the delimiter used in a delimited text file."""
    sniffer = Sniffer()
    data = io.open(file_path, mode="r", encoding="latin-1").read(bytes)
    return sniffer.sniff(data).delimiter


def replace_csv_column_names(
    csv_file: Path | str, column_name_mapper: dict[str, str] | None = None
) -> None:
    """Replace column headers in a CSV file based on a mapping."""
    from .io import get_delimiter  # local import to avoid cycles

    if column_name_mapper is None:
        column_name_mapper = {}

    delimiter = get_delimiter(csv_file, bytes=16000)
    with open(csv_file, "r") as f:
        first_line = f.readline().strip()

    first_line_items = first_line.split(delimiter)
    new_first_line_items = []
    for first_line_item in first_line_items:
        for key, item in column_name_mapper.items():
            if first_line_item == key:
                first_line_item = item
        new_first_line_items.append(first_line_item)
    new_first_line = delimiter.join(new_first_line_items)

    with open(csv_file, "r") as f:
        lines = f.readlines()
    lines[0] = new_first_line + "\n"
    with open(csv_file, "w") as f:
        f.writelines(lines)


def unzip_hmda_file(
    zip_file: Path | str, raw_folder: Path | str, replace: bool = False
) -> Path:
    """Extract a compressed HMDA archive and return extracted file path."""
    from .io import replace_csv_column_names  # local import

    zip_file = Path(zip_file)
    raw_folder = Path(raw_folder)
    if zip_file.suffix.lower() != ".zip":
        zip_file = zip_file.with_suffix(".zip")
        if not zip_file.exists():
            raise ValueError(
                "The file name was not given as a zip file. Failed to find a comparably-named zip file."
            )

    with zipfile.ZipFile(zip_file) as z:
        delimited_files = [
            x for x in z.namelist() if (x.endswith(".txt") or x.endswith(".csv")) and "/" not in x
        ]
        for file in delimited_files:
            raw_file_name = raw_folder / file
            if (not raw_file_name.exists()) or replace:
                logger.info("Extracting file: %s", file)
                try:
                    z.extract(file, path=raw_folder)
                except Exception:
                    logger.warning(
                        "Could not unzip file: %s with zipfile. Using 7z instead.", file
                    )
                    unzip_string = "C:/Program Files/7-Zip/7z.exe"
                    p = subprocess.Popen(
                        [unzip_string, "e", str(zip_file), f"-o{raw_folder}", file, "-y"]
                    )
                    p.wait()

            if "panel" in file:
                column_name_mapper = {
                    "topholder_rssd": "top_holder_rssd",
                    "topholder_name": "top_holder_name",
                    "upper": "lei",
                }
                replace_csv_column_names(raw_file_name, column_name_mapper=column_name_mapper)

    return raw_file_name


__all__ = [
    "normalized_file_stem",
    "should_process_output",
    "get_delimiter",
    "replace_csv_column_names",
    "unzip_hmda_file",
]


