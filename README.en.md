# NP-COMPANY-PROCESS

## Functionality

This project provides notebooks to process and identify tenders and companies, as well as to disambiguate the companies that compose an UTE.

## File Structure

### Data

The directory defined with the data used by the application is by default: **_data_**. Inside that directory, you'll find other subdirectories, mainly metadata. For example:

```bash
data
└───metadata
    ├───insiders.parquet
    ├───outsiders.parquet
    └───minors.parquet
└───company_info.parquet
└───utes.parquet
```

### app/src

It contains various functionalities of the application, divided into companies, nif_validation, and utils.

#### companies

Contains functions to identify the type of company and unify their notation: everything is standardized to its acronym; for example: *sociedad limitada*, *soc. limitada*, *s. l.* become *s.l.*. It also allows for the removal of this from the text if desired.

#### nif_validation

Allows checking if the various identifiers (DNI, NIE, and CIF) are correct, as well as identifying which one is used and obtaining the data based on that.\
The information that appears in the BOE has been used to create these methods, mainly through references from https://es.wikipedia.org/wiki/N%C3%BAmero_de_identificaci%C3%B3n_fiscal

#### utils

Here you'll find functions with various purposes, such as parallelizing or loading data.

### ute_resolver

This directory contains the scripts that implement the disambiguation of UTEs. The output of this process is two tables: ``utes_des.parquet`` and ``companies_des.parquet`` have been generated based on the information available in ``company_info.parquet`` and ``utes.parquet``. The former contains the companies composing each UTE. It's important to note that there may be cases of false positives, where companies erroneously appear associated with a UTE, as well as cases where companies are missing (e.g., in a UTE composed of 3 companies, only 2 are listed in the table). False positives are particularly common when a company name is generic (e.g., 'Jose', 'Maria', etc.). The ``companies_des.parquet`` table contains, for each company, the UTEs in which the algorithm suggests they have participated.

```bash
ute_resolver
├── metalists
│   ├── apellidos.parquet
│   ├── catalan.txt
│   ├── es.txt
│   ├── filtered_elements.txt
│   └── nombres.parquet
├── calculate_coverage.py
├── disambiguate_utes.py
└── generate_stops.py
├── test_zaragoza.ipynb
```

The script ``disambiguate_utes.py`` is the one in charge of the generation of the ``utes_des.parquet`` and ``companies_des.parquet`` files with the disambiguation results. It offers two distinct approaches for disambiguating UTEs:

- **Rule-based Fuzzy Matching:** The first approach employs a set of predefined rules to segment the names of UTEs and companies. Utilizing the fuzzywuzzy library, it identifies UTEs that closely resemble the segmented components of company names.

- **Recursive Substring Fuzzy Matching:** The second approach combines recursive substring searches within UTE names with rule-based segmentation of UTE and company names. It then identifies partial matches with punctuation separation, after filtering out irrelevant terms. These exclusions include:
  - Words containing fewer than three characters.
  - Numerical values.
  - Common Spanish and Catalan words sourced from ``es.txt`` and ``catalan.txt``.
  - Frequently occurring Spanish surnames and given names extracted from ``apellidos.parquet`` and ``nombres.parquet`` files.
  - Common terms derived from the splits of company names with frequencies above the 95th percentile, obtained from ``filtered_elements.txt.`` This list is generated using the generate_stops.py script.

For efficient computation, this method requires the use of Spark.
Upon disambiguating UTEs, each company name corresponds to a list of associated UTEs. The script generates a new DataFrame containing UTEs and their respective associated companies (integrantes).

The second approach method offers a larger coverage compared to the first approach. Despite its broader scope, it may generate false positives.

### Notebooks

#### match_tender.ipynb

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

#### match_companies.ipynb

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
