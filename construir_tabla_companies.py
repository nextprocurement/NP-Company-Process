import json
import time
from collections import Counter
from itertools import chain
from pathlib import Path
import numpy as np
import pandas as pd
import regex
import contextlib

from src.companies.processor import clean_company_type, normalize_company_name
from src.nif_validation.validation import (
    get_info_from_cif,
    get_nif_type,
    is_valid_nif,
    validate_nif,
)
from src.utils.utils import fill_to_length, merge_orig_dataframes
from src.utils.utils_parallelization import parallelize_function

def nif_from_name(name):
    """Searches whether the NIF is included in the name and separates it"""
    name_spl = np.array(name.split())
    valid = np.array([bool(validate_nif(s)) for s in name_spl])
    new_name = " ".join(name_spl[~valid])
    new_nif = Counter(name_spl[valid]).most_common()[0][0] if valid.any() else np.nan
    return new_name, new_nif

@contextlib.contextmanager
def log_time(task_name: str):
    """Context manager to log the execution time of a block of code."""
    t0 = time.time()
    yield
    t1 = time.time()
    print(f"{task_name} - {t1-t0}")
   
def execute_function(func, data, prefer=None, workers=-1, *args, **kwargs):
    """Wrapper function to decide whether to use parallel processing or not."""
    if not prefer:
        return data.apply(func, *args, **kwargs)
    else:
        return parallelize_function(
            func,
            data,
            workers=workers,
            prefer=prefer,
            show_progress=True,
            leave=True,
            position=0,
            *args,
            **kwargs,
        )

def clean_df(df: pd.DataFrame, prefer=None, workers=-1):
    # Remove unwanted whitespace
    with log_time("Removing unwanted whitespace"):
        df = df.applymap(
            lambda x: regex.sub(r"((?<=\w+\W)\s+)|(\s+(?=\W\w+))", "", x)
            if not pd.isna(x)
            else None
        )

    # Validate NIF
    with log_time("Validating NIF"):
        df["ID"] = execute_function(validate_nif, df["ID"], prefer, workers)

    # Clean company type
    with log_time("Cleaning company type"):
        name = [
            regex.sub(i, "", n) if not (pd.isna(n) or pd.isna(i)) else n
            for i, n in df[["ID", "Name"]].values
        ]
        df["Name"] = execute_function(
            clean_company_type, name, prefer, workers, remove_type=False
        )

    # Remove company type
    with log_time("Removing company type"):
        df["Name_proc"] = execute_function(
            clean_company_type, df["Name"], prefer, workers, remove_type=True
        )

    # Normalize company name
    with log_time("Normalizing company name"):
        df["Name_norm"] = execute_function(
            normalize_company_name, df["Name_proc"], prefer, workers
        )

    return df

# Choose definitive values
def suggest_value(elements):
    """
    Select elements based on appearance.
    If same number of appearances, choose the longest.
    If shorter elements are not included in the 'main' one, return all.
    """
    cnt = Counter(elements)
    cnt.pop(None, None)
    cnt = cnt.most_common()
    if cnt:
        max_cnt = cnt[0][1]
        els = sorted([k for k, v in cnt if v == max_cnt], key=lambda x: (-len(x), x))
        # return els[0]
        base = els.pop(0)
        return [base]
        # if all(
        #     [all(t in base for t in regex.sub(r"\W", " ", el).split()) for el in els]
        # ):
        #     return [base]
        # return [base] + els
    else:
        # return None
        return [None]

# Repeated IDs
def unify_repeated_col(df: pd.DataFrame, rep_col: str, un_col: str):
    """
    Takes a dataframe with duplicated values in one column that should be unique (e.g. repeated IDs)
    and another column that should also be unique given the previous one (e.g. title)
    and unifies it so that it chooses the best option.

    Parameters
    ----------
    df: pd.DataFrame
    rep_col: str
        Name of column with repeated values that will be unified
    un_col: str
        Name of column with non unique values
    """
    # Non-unique columns
    cols_vals = [c for c in df.columns if c not in [rep_col, "count", "index"]]
    repeated_rows = df[rep_col].duplicated(keep=False)
    repeated = df[repeated_rows]

    # Count times the values appear
    repeated.loc[repeated.index, [un_col]] = (
        repeated.loc[repeated.index, un_col].apply(lambda x: [x])
        * repeated.loc[repeated.index, "count"]
    )
    # Group by repeated
    repeated = repeated.reset_index()
    repeated = repeated.groupby(rep_col).agg(
        {
            # "index": list,
            "index": sum,
            **{c: lambda x: list(chain.from_iterable(x)) for c in cols_vals},
            "count": sum,
        }
    )
    # Get the most common values for each column
    repeated.loc[repeated.index, un_col] = (
        repeated.loc[repeated.index, un_col].apply(suggest_value).values
    )
    repeated = repeated.reset_index()

    # Concatenate unique
    use_index = repeated.loc[repeated[un_col].apply(len) == 1, un_col].index
    repeated.loc[use_index, un_col] = repeated.loc[use_index, un_col].apply(
        lambda x: x[0]
    )
    unified = repeated.loc[use_index]

    return unified

def isPYME(SMEIndicators):
    # Evaluate if is SME based on the SMEAwardedIndicator appearances
    # If True and False are present, return None
    # TODO: make a better decision
    sme_counts = Counter(SMEIndicators)
    if True in sme_counts and False in sme_counts:
        return None
    return sme_counts.most_common(1)[0][0]

def get_city_name(CityName):
    # Evaluate the city name based on the CityName appearances
    # Get most common excluding None
    # TODO: make a better decision
    city_names = Counter(CityName)
    if None in city_names.keys():
        city_names.pop(None)
    if not len(city_names) == 1:
        return None
    return city_names.most_common(1)[0][0]

def get_postal_zone(PostalZone):
    # Evaluate the postal zone based on the PostalZone appearances
    # Get most common excluding None
    # TODO: make a better decision
    postal_zones = Counter(PostalZone)
    if None in postal_zones.keys():
        postal_zones.pop(None)
    if not len(postal_zones) == 1:
        return None
    return postal_zones.most_common(1)[0][0].split(".")[0]

def main():
    dir_path = Path("data/metadata/")
    df_companies = merge_orig_dataframes(dir_metadata=dir_path)
    print(df_companies)
    # Use only those where all dimensions match
    # (e.g. same number of companies and companies ids)
    # and drop NAs
    df_companies = df_companies[
        df_companies[["ID", "Name"]]
        .applymap(lambda x: not pd.isna(x[0]))
        .apply(all, axis=1)
    ]
    df_companies = df_companies[
        df_companies.applymap(lambda x: len(x) if x[0] else None).apply(
            lambda x: len(set([el for el in x if not pd.isnull(el)])) == 1,
            axis=1,
        )
    ]
    companies_columns = list(df_companies.columns)
    # Get number of companies by tender
    df_companies["_len"] = df_companies["ID"].apply(len)

    # Fill lists of None to have the same number of elements and explode later
    companies = pd.DataFrame(
        df_companies.apply(
            lambda x: [fill_to_length(list(el), x[-1]) for el in x[:-1]], axis=1
        ).tolist(),
        columns=companies_columns,
    )
    # Split companies in rows
    companies = companies.explode(companies_columns)
    companies = companies.reset_index(drop=True)
    
    with log_time("Clean df"):
        companies_clean = clean_df(companies, prefer="processes", workers=-1)
    
    # Aggregate company info in lists
    companies_clean["SMEAwardedIndicator"] = companies_clean["SMEAwardedIndicator"].apply(
        lambda x: None if not x else True if x == "true" else False
    )
    companies_clean = (
        companies_clean
        # companies[["ID", "Name", "Name_proc", "Name_norm"]]
        .groupby(["ID", "Name_norm"])
        .agg(list)
        .reset_index()
    )
    companies_clean["count"] = companies_clean["Name_proc"].apply(len)
    companies_clean = companies_clean.reset_index()
    
    # Unique names and IDs
    # These companies have always appeared with the same (id-name) association
    cols_vals = [
        c for c in companies_clean.columns if c not in ["ID", "Name_norm", "count"]
    ]
    unique_ID = ~companies_clean["ID"].duplicated(keep=False)
    unique_NAME = ~companies_clean["Name_norm"].duplicated(keep=False)

    # Unique by ID and name
    unique = companies_clean[unique_ID & unique_NAME].copy()

    # Non unique IDs
    non_unique_ids = list(set(companies_clean["index"]) - set(unique["index"]))
    non_unique = companies_clean[companies_clean["index"].isin(non_unique_ids)].copy()

    unique["index"] = unique["index"].apply(lambda x: [x])
    non_unique["index"] = non_unique["index"].apply(lambda x: [x])
    print(unique.shape, non_unique.shape)
    
    # Obtain unique ID-name
    unified_ID = unify_repeated_col(non_unique, "ID", "Name_norm")
    # Update non_unique
    non_unique_ids = list(
        set(chain.from_iterable(non_unique["index"]))
        - set(chain.from_iterable(unified_ID["index"]))
    )
    # non_unique = companies_clean.loc[non_unique_ids]
    non_unique = companies_clean[companies_clean["index"].isin(non_unique_ids)]
    non_unique["index"] = non_unique["index"].apply(lambda x: [x])
    
    # Obtain unique name-ID
    unified_NAME = unify_repeated_col(non_unique, "Name_norm", "ID")
    # Update non_unique
    non_unique_ids = list(
        set(chain.from_iterable(non_unique["index"]))
        - set(chain.from_iterable(unified_NAME["index"]))
    )
    # non_unique = companies_clean.loc[non_unique_ids]
    non_unique = companies_clean[companies_clean["index"].isin(non_unique_ids)]
    non_unique["index"] = non_unique["index"].apply(lambda x: [x])
    
    # Global
    # Merge unique+unifiedID+unifiedName+nonUnique
    merged_global = pd.concat([unique, unified_ID, unified_NAME, non_unique])
    cols_vals = [
        c
        for c in merged_global.columns
        if c not in ["ID", "Name_norm", "count", "index", "id_tender"]
    ]
    merged_global = merged_global.groupby(["ID", "Name_norm"]).agg(
        {
            # "index": lambda x: list(chain.from_iterable(x)),
            "index": sum,
            "id_tender": sum,
            **{c: lambda x: list(chain.from_iterable(x)) for c in cols_vals},
            "count": sum,
        }
    )
    merged_global = merged_global.reset_index()
    print(len(merged_global))

    # Get all names found in the tenders
    merged_global["UsedNames"] = (merged_global["Name"] + merged_global["Name_proc"]).apply(
        lambda x: sorted(list(set(x)))
    )

    # Initial computations
    data = merged_global["Name_proc"]
    # local_frequencies = data.apply(lambda x: dict(Counter(x)))
    local_frequencies = data.apply(lambda x: {k: v / len(x) for k, v in Counter(x).items()})
    global_frequencies = data.explode().value_counts().to_dict()
    global_frequencies = pd.Series(global_frequencies)
    merged_global["Name_proposed"] = local_frequencies.apply(
        lambda x: sorted(x.items(), key=lambda el: el[1], reverse=True)[0][0]
    )
    merged_global["isPYME"] = merged_global["SMEAwardedIndicator"].apply(isPYME)
    merged_global["City"] = merged_global["CityName"].apply(get_city_name)
    merged_global["PostalCode"] = merged_global["PostalZone"].apply(get_postal_zone)
    
    # Add information based on NIF
    merged_global["NIF_type"] = merged_global["ID"].apply(get_nif_type)
    merged_global["prov"], merged_global["comp_type"], merged_global["comp_desc"] = list(
        zip(*merged_global["ID"].apply(get_info_from_cif))
    )
    merged_global["comp_type"] = merged_global["comp_type"].apply(
        lambda x: x.split(",")[0] if not pd.isna(x) else None
    )

    # Find UTEs based on name
    ute_n = merged_global["UsedNames"].apply(
        lambda x: bool(regex.search(r"\bu(\.)?t(\.)?e(\.)?\b", " ".join(x)))
    )
    # Find UTEs based on ID
    ute_i = merged_global["ID"].apply(lambda x: x.startswith("u"))

    utes = merged_global[ute_i | ute_n]
    
    merged_global['FullName'] = merged_global['UsedNames'].apply(lambda x: max(x, key=len))

    provisional_company_info = merged_global.rename(
    columns={
        "ID": "NIF",
        "id_tender": "id_tender",
        "Name_proposed": "Name1",
        "prov": "Province",
        "NIF_type": "NIFtype",
        "comp_type": "CompanyType",
        "comp_desc": "CompanyDescription",
    }
    )[
    [
        "NIF",
        "FullName",
        "Name1",
        "Province",
        "NIFtype",
        "CompanyType",
        "CompanyDescription",
        "id_tender",
    ]
    ]
    provisional_company_info = provisional_company_info.rename(columns={
        "Name1": "Name"  # Renombrando Name1 a Name
    }
    )

    provisional_company_info.to_parquet("data/provisional_company_info.parquet")
    utes.to_parquet("data/utes.parquet")

if __name__ == "__main__":
     main()
    
    