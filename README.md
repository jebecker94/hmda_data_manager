# hmda_data_manager
Tools for managing CFPB's Home Mortgage Disclosure Act (HMDA) data for research projects.

# Overview
This project is designed to streamline the use of the HMDA data for academic research purposes.

# Motivation
The HMDA data files are large and unweildy. To my knowledge, there are few resources for managing these files for use in research projects.

# Functionality
Major functions on this project include:
1. Managing downloads
   - Tracking changes across major revisions of files
2. Streamlined reading and storage
   - Convert large files to parquet, with appropriate column types and convenient partitioning
3. Standardizing variables, detecting outliers, and fixing data errors
   - Year-by-reporter checks for systematic data errors (e.g., incorrect conventions for reporting rates, points+fees, etc.)
4. Creating derived datasets
   - Linking originated and purchased loans within and across years
