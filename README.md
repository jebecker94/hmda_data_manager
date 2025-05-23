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

# The HMDAIndex Variable
As part of the data ingestion process, these import scripts create a new variable called 'HMDAIndex' for data released starting in 2018.

Construction:
The HMDAIndex variable has format YYYYt_#########, which is constructed from three components:
- The data coverage year YYYY: The four-digit year covered by the data.
- The file type code t: A one-digit alphanumeric character identifying the HMDA file type. This takes values of 'a' for three-year files, 'b' for the one-year files, 'c' for the snapshot files, and 'd' for the (preliminary) MLAR files.
- The row number #########: A nine-digit number identifying the row number in the HMDA file for year YYYY and type code t. Note that values are left-padded with zeros and begin with 000000000.

Purpose:
There are no unique identifiers in the HMDA data files released starting in 2018. This makes it difficult to share code and derived datasets between researchers using their own versions of the HMDA files. The HMDAIndex variable follows a straightforward construction procedure and makes it easy for any researcher to reconsruct a unique identifier to reference specific HMDA observations in derived datasets like crosswalks.

# Matching HMDA Sellers and Purchasers
HMDA's reporting requirements mean that loans which are originated and sold to a non-GSE are reported twice--once as an originated loan (action_taken==1) and once as a purchased loan (action_taken==6).

We provide preliminary matching scripts that match loan originations to subsequent loan purchases.

This can be used in a number of ways:
- Studying seller-purchaser relationships in the wholesale mortgage market
- Improving matches between HMDA and other datasets by providing a better lender identifier for subsequent matches (e.g., matching forward to MBS transactions)

# How to Use This Project
In order to use this project, there are a few manual steps to take before you can run the code.

1. Download HMDA data
- Navigate to the HMDA data website and download the static files you with to use.
2. Place the zip files in the raw data folder
