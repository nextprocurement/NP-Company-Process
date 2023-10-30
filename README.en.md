# NextProcurement

# Functionality
This project provides notebooks to process and identify tenders and companies.

# File Structure
## Data:
The directory defined with the data used by the application is by default: **_data_**. Inside that directory, you'll find other subdirectories, mainly metadata. For example:
```
data
└───metadata
    ├───insiders.parquet
    ├───outsiders.parquet
    └───minors.parquet
```
## app/src
It contains various functionalities of the application, divided into companies, nif_validation, and utils.

### companies:
Contains functions to identify the type of company and unify their notation: everything is standardized to its acronym; for example: *sociedad limitada*, *soc. limitada*, *s. l.* become *s.l.*. It also allows for the removal of this from the text if desired.

### nif_validation
Allows checking if the various identifiers (DNI, NIE, and CIF) are correct, as well as identifying which one is used and obtaining the data based on that.\
The information that appears in the BOE has been used to create these methods, mainly through references from https://es.wikipedia.org/wiki/N%C3%BAmero_de_identificaci%C3%B3n_fiscal

### utils
Here you'll find functions with various purposes, such as parallelizing or loading data.

## Notebooks
### match_tender.ipynb
This notebook matches tenders from PLACE and GENCAT, in this case. The step-by-step of the notebook is as follows:
1. Load data from PLACE and GENCAT.
2. Functions for cleaning the dataframes and selecting relevant columns.
3. Collect PLACE data
    - **Get relevant data**
        - Unify columns
        - Split CPVs
        - Clean columns
        - Create a `cpv_div` column for use in the merge
    - **Get data from cat**
        - Obtain potential data from Catalonia
        - Combine them by ID and only keep those with the same title
4. Repeat the previous step for GENCAT data
5. Make an association by ID and use only ["id_orig", "title", "cpv_div", "index_agg", "index_cat"], where index are the indices of the cleaned dataframes obtained in the previous point (`use_tend_agg` and `use_tend_cat`). A counter is made for titles and cpvs to later keep the most common one.
6. Repeat the association but using the columns of `title` and `cpv_div`.
7. Combine both dataframes obtaining a counter of the remaining elements. Later on, this counter will be used to obtain the most probable data.
8. From this point, you can search in one dataframe or another, fill in data, etc.

### test_company_proc.ipynb
This notebook matches companies. The step-by-step of the notebook is as follows:
1. Imports and functions to separate the NIF when included in the text and to clean the dataframe.
2. Combine the different sources (minors, insiders, outsiders)
3. Filter to only use companies that contain a name and NIF (NaNs are discarded)
4. There's a section with foreign companies that can be explored
5. Data cleaning (validate NIF, standardize texts, etc.)
6. Combine by ID and name and determine if it's an SME
7. Separate companies that have unique ID and names
8. Unique values are used directly. In repeated values, a name is assigned to each identifier based on the times it appears
9. Both dataframes (unique and non-unique) are concatenated
10. A name is proposed from all that appear (to be reviewed)
11. Expand information based on the data:
    - Is an SME
    - City and postal code
    - Province and type of company
12. Search for UTEs (can be added directly as a column)
13. Load companies from Zaragoza and do the same cleaning
14. Overview:
    - See which names and IDs are repeated
    - Search in Zaragoza's companies from those we had before (by ID and name)
    - Companies without NIF and try to fill them with the NIFs we have
    - Separation between unique and repeated values (ID-Name).


[![](https://img.shields.io/badge/lang-en-red)](README.en.md)
