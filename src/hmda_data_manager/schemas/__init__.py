"""
HMDA Data Schema Documentation
===============================

This module provides access to HTML schema documentation files for different
HMDA data formats and time periods. These files contain the official CFPB
field definitions, data types, and valid values for HMDA data.

Available Schema Files
----------------------
- hmda_lar_schema_2007-2017.html: LAR fields for 2007-2017 period
- hmda_lar_schema_post2018.html: LAR fields for 2018+ period  
- hmda_panel_schema_post2018.html: Panel fields for 2018+ period
- hmda_ts_schema_post2018.html: Transmittal Series fields for 2018+ period

Functions
---------
- get_schema_path: Get the path to a specific schema file
- list_available_schemas: List all available schema files

Notes
-----
Schema files are used by the import functions to determine appropriate
data types when reading HMDA files.
"""

from pathlib import Path


def get_schema_path(schema_name: str) -> Path:
    """
    Get the path to a specific HMDA schema file.
    
    Parameters
    ----------
    schema_name : str
        Name of the schema file (with or without .html extension)
        
    Returns
    -------
    Path
        Full path to the schema file
        
    Raises
    ------
    FileNotFoundError
        If the schema file does not exist
        
    Examples
    --------
    >>> get_schema_path("hmda_lar_schema_post2018.html")
    Path('src/hmda_data_manager/schemas/hmda_lar_schema_post2018.html')
    
    >>> get_schema_path("hmda_lar_schema_post2018")  # Extension optional
    Path('src/hmda_data_manager/schemas/hmda_lar_schema_post2018.html')
    """
    # Add .html extension if not present
    if not schema_name.endswith('.html'):
        schema_name += '.html'
        
    schema_path = Path(__file__).parent / schema_name
    
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
    return schema_path


def list_available_schemas() -> list[str]:
    """
    List all available HMDA schema files.
    
    Returns
    -------
    list[str]
        List of available schema file names
        
    Examples
    --------
    >>> list_available_schemas()
    ['hmda_lar_schema_2007-2017.html', 'hmda_lar_schema_post2018.html', ...]
    """
    schema_dir = Path(__file__).parent
    return [f.name for f in schema_dir.glob("*.html")]


__all__ = [
    "get_schema_path",
    "list_available_schemas",
]

