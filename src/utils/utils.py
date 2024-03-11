from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

# This file contains the necessary functions to read, process and merge
# the parquet file with the available information


def fill_to_length(iterable, l):
    """Fill an iterable to a certain length `l`."""
    return iterable + [None] * (l - len(iterable))


def unify_colname(col):
    return ".".join([el for el in col if el])


def suggest_value(elements):
    """
    Select elements based on appearance.
    If same number of appearances, choose the longest.
    If shorter elements are not included in the 'main' one, return all.
    """
    cnt = Counter(filter(None, elements))
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


def fill_na(cell, fill=[]):
    """
    Fill elements in pd.DataFrame with `fill`.
    """
    if hasattr(cell, "__iter__"):
        if isinstance(cell, str):
            if cell == "nan":
                return fill
            return cell
        nas = [True if el == "nan" or pd.isna(el) else False for el in cell]
        if all(nas):
            return fill
        return cell
    if pd.isna(cell):
        return fill
    return cell


def evaluate_cell(cell):
    """Evaluate the cell and return a list."""
    if not isinstance(cell, (list, np.ndarray)):
        cell = [cell]
    if pd.isnull(cell[0]):
        return [None]
    elif isinstance(cell[0], str) and cell[0].startswith("[") and cell[0].endswith("]"):
        return eval(cell[0])
    else:
        return cell


def process_dataframe(df):
    """Process a single dataframe."""
    # Unify indices
    index_names = df.index.names
    orig_cols = df.columns
    df.reset_index(inplace=True)
    df["identifier"] = df[index_names].astype(str).agg("/".join, axis=1)
    df.set_index("identifier", inplace=True)
    df = df[orig_cols]

    # Unify columns
    join_str = lambda x: ".".join([el for el in x if el])
    joint_cnames = {join_str(c): c for c in df.columns}
    reverse_joint_cnames = {v: k for k, v in joint_cnames.items()}
    comp_cols = sorted(
        [
            v
            for k, v in joint_cnames.items()
            if "WinningParty" in k or k == "id" or "SMEAwardedIndicator" in k
        ]
    )
    use_cols = [reverse_joint_cnames[c].split(".")[-1] for c in comp_cols]

    df_companies = df[comp_cols]
    df_companies.columns = use_cols

    # Clean columns
    df_companies = df_companies.applymap(fill_na, fill=[None])
    for c in df_companies.columns:
        # Si la columna es 'id', se omite la conversión a minúsculas
        if c == 'id':
            df_companies[c] = df_companies[c].apply(evaluate_cell).apply(
                lambda x: [None] if not x[0] else [str(el).strip() for el in x]
            )
        else:
            df_companies[c] = df_companies[c].apply(evaluate_cell).apply(
                lambda x: [None] if not x[0] else [str(el).strip().lower() for el in x]
            )

    return df_companies.rename(columns={"id": "id_tender"})


def merge_orig_dataframes(
    dir_metadata: Path, merge_dfs=["minors", "insiders", "outsiders"]
):
    """Merge original data parquet files into single dataframe."""
    dfs = [pd.read_parquet(dir_metadata.joinpath(f"{d}.parquet")) for d in merge_dfs]
    dfs_companies = [process_dataframe(df) for df in dfs]

    df_companies = pd.concat(dfs_companies)
    df_companies = df_companies.applymap(lambda x: [None] if x is np.nan else x)

    return df_companies
