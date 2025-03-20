# NP-COMPANY-PROCESS

[![PyPI - License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/nextprocurement/NP-Company-Process/blob/main/LICENSE)

This project provides notebooks to process and identify tenders and companies, as well as to disambiguate the companies that compose an UTE.

## File Structure

### Data

The directory defined with the data used by the application is by default: **_data_**. Inside that directory, you'll find other subdirectories, mainly metadata. For example:

```bash
data
├── metadata
│   ├── insiders.parquet
│   ├── outsiders.parquet
│   └── minors.parquet
├── company_info.parquet
└── utes.parquet
```

### app/src

It contains various functionalities of the application, divided into companies, nif_validation, ute_resolver, and utils.

#### companies

Contains functions to identify the type of company and unify their notation: everything is standardized to its acronym; for example: *sociedad limitada*, *soc. limitada*, *s. l.* become *s.l.*. It also allows for the removal of this from the text if desired.

#### nif_validation

Allows checking if the various identifiers (DNI, NIE, and CIF) are correct, as well as identifying which one is used and obtaining the data based on that.\
The information that appears in the BOE has been used to create these methods, mainly through references from https://es.wikipedia.org/wiki/N%C3%BAmero_de_identificaci%C3%B3n_fiscal

#### utils

Here you'll find functions with various purposes, such as parallelizing or loading data.

### ute_resolver

This directory contains the scripts that implement the disambiguation of UTEs. The output of this process is two tables: ``utes_des.parquet`` and ``companies_des.parquet`` have been generated based on the information available in ``company_info.parquet`` and ``utes.parquet``. The former contains the companies composing each UTE. It's important to note that there may be cases of false positives, where companies erroneously appear associated with a UTE, as well as cases where companies are missing (e.g., in a UTE composed of 3 companies, only 2 are listed in the table). False positives are particularly common when a company name is generic (e.g., 'Jose', 'Maria', etc.). The ``companies_des.parquet`` table contains, for each company, the UTEs in which the algorithm suggests they have participated.

Two distinct approaches for disambiguating UTEs are offered:

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

---

### Main Scripts

#### 1) `calculate_coverage.py`
This script calculates the coverage of the UTEs found by two different methods. Coverage is determined as the percentage of UTEs found by each method relative to the total number of UTEs in the dataset. It also provides examples of UTEs detected by each method.

**Usage:**
```bash
python calculate_coverage.py --path_data <path_to_data> --compare1 df_not_in_utes_approach1.parquet --compare2 companies_des.parquet --utes utes.parquet --nshow 10
```
- `--path_data`: Path to the data folder (default: `data`).
- `--compare1`: First file to compare (default: `df_not_in_utes_approach1.parquet`).
- `--compare2`: Second file to compare (default: `companies_des.parquet`).
- `--utes`: File containing the UTEs (default: `utes.parquet`).
- `--nshow`: Number of examples to display (default: `10`).

#### 2) `create_company_table.py`
This script processes company data, cleans company names, validates NIFs, and generates a unified company table. It standardizes company identifiers and helps with company name normalization.

**Usage:**
```bash
python create_company_table.py --path_archivos <path_to_data> --download_path <output_path>
```
- `--path_archivos`: Path to input data files (default: `/export/usuarios_ml4ds/cggamella/NP-Company-Process/data/DESCARGAS_MAYO`).
- `--download_path`: Output directory for processed company tables (default: `/export/usuarios_ml4ds/cggamella/NP-Company-Process/data`).

#### 3) `disambiguate_utes.py`
This script offers the two different approaches for UTE disambiguation.

**Usage:**
```bash
python disambiguate_utes.py --path_data <path_to_data> --path_aux <auxiliary_data_path> --approach <1_or_2>
```
- `--path_data`: Path to the data folder.
- `--path_aux`: Path to auxiliary datasets (e.g., name frequency lists).
- `--approach`: Approach to use (`1` for Rule-based Fuzzy Matching, `2` for Recursive Substring Matching).

> **Note:** Approach 2 requires **Spark** for efficient computation.

---

## Acknowledgements

This work has received funding from the NextProcurement European Action (grant agreement INEA/CEF/ICT/A2020/2373713-Action 2020-ES-IA-0255).

<p align="center">
  <img src="static/Images/eu-logo.svg" alt="EU Logo" height=100 width=200>
  <img src="static/Images/nextprocurement-logo.png" alt="Next Procurement Logo" height=100 width=200>
</p>

