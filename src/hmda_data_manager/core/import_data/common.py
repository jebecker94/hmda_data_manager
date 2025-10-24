"""
Common utilities and constants shared across HMDA import modules.

This module contains only truly universal constants used by all time-period
specific import modules. All other utilities have been moved to their
respective period-specific modules for better organization and readability.
"""

# Shared constants used across all time periods
CENSUS_TRACT_COLUMN = "census_tract"
HMDA_INDEX_COLUMN = "HMDAIndex"
