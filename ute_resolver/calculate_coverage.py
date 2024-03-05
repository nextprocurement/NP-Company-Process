"""
This script calculates the coverage of the UTEs found by two different methods. The coverage is calculated as the percentage of UTEs found by the method with respect to the total number of UTEs. The script also shows some examples of the UTEs found by each method.

Author: Lorena Calvo-Bartolomé
Date: 15/02/2024
"""
import pandas as pd
import pathlib
import argparse
from tabulate import tabulate


def calculate_coverage(
        filtered_method: pd.DataFrame,
        df_utes: pd.DataFrame
) -> float:
    """Calculate the coverage of the UTEs found by the method with respect to the total number of UTEs. The coverage is calculated as the percentage of UTEs found by the method with respect to the total number of UTEs.

    Parameters
    ----------
    filtered_method : pd.DataFrame
        The filtered DataFrame with the UTEs found by the method for each company.
    df_utes : pd.DataFrame
        The original DataFrame with the UTEs.

    Returns
    -------
    float
        The coverage of the UTEs found by the method with respect to the total number of UTEs.
    """
    utes_found = [ute for el in filtered_method.utes.values.tolist()
                  for ute in el]
    utes_matching = (len(list(set(utes_found))) / len(df_utes)) * 100
    return utes_matching


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--path_data", type=str,
                        help="Path to the data folder.",
                        default="../data")
    parser.add_argument("--compare1", type=str,
                        help="Name of the first file to compare.", default="df_not_in_utes_approach1.parquet")
    parser.add_argument("--compare2", type=str,
                        help="Name of the second file to compare", default="companies_des.parquet")
    parser.add_argument("--utes", type=str,
                        help="Name of the file with the utes.",
                        default="utes.parquet")
    parser.add_argument("--nshow", type=int,
                        help="Number of examples to show", default=10)

    args = parser.parse_args()

    # Load the data from the two methods and the original UTEs
    path_data = pathlib.Path(args.path_data)
    path_found_method1 = path_data.joinpath(args.compare1)
    path_found_method2 = path_data.joinpath(args.compare2)
    path_utes = path_data.joinpath(args.utes)
    df_not_in_utes_method1 = pd.read_parquet(path_found_method1)
    df_not_in_utes_method2 = pd.read_parquet(path_found_method2)
    df_utes = pd.read_parquet(path_utes)

    # Calculate the length of the utes found, that is, the number of UTEs found for each company
    df_not_in_utes_method1["utes_length"] = \
        df_not_in_utes_method1["utes"].apply(len)
    df_not_in_utes_method2["utes_length"] = \
        df_not_in_utes_method2["utes"].apply(len)

    # Filter the companies that have found UTEs
    filtered_method1 = df_not_in_utes_method1[(
        df_not_in_utes_method1.utes_length > 0)]
    filtered_method2 = df_not_in_utes_method2[(
        df_not_in_utes_method2.utes_length > 0)]

    utes_matching_method1 = calculate_coverage(filtered_method1, df_utes)
    print(utes_matching_method1)

    utes_matching_method2 = calculate_coverage(filtered_method2, df_utes)
    print(utes_matching_method2)

    coverage_results = [
        ["Method 1", utes_matching_method1],
        ["Method 2", utes_matching_method2]
    ]
    print("-- Coverage Results:")
    print(tabulate(coverage_results, headers=[
        "Method", "Coverage (%)"], tablefmt="mixed_grid"))

    table_filtered_method1 = filtered_method1.head(args.nshow)[
        ["Name", "utes"]]
    table_filtered_method2 = filtered_method2.head(args.nshow)[
        ["Name", "utes"]]

    sample_results_method1 = [
        (table_filtered_method1.iloc[i]["Name"], "\n".join(table_filtered_method1.iloc[i]["utes"])) for i in range(args.nshow)
    ]

    sample_results_method2 = [
        (table_filtered_method2.iloc[i]["Name"], "\n".join(table_filtered_method2.iloc[i]["utes"])) for i in range(args.nshow)
    ]


    print("-- Some examples using method 1:")
    print(tabulate(sample_results_method1, headers=[
        "Company Names", "Utes"], tablefmt="mixed_grid"))

    print("*"*100)
    print("*"*100)

    print("-- Some examples using method 2:")
    print(tabulate(sample_results_method2, headers=[
        "Company Names", "Utes"], tablefmt="mixed_grid"))

    return


if __name__ == "__main__":
    main()
