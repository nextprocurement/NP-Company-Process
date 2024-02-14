import pandas as pd
import pathlib
from tabulate import tabulate


path_found_method1 = pathlib.Path("../data/df_not_in_utes_enriched1.parquet")
path_found_method2 = pathlib.Path("../data/utes_spark.parquet")
path_utes = pathlib.Path("../data/utes.parquet")

df_not_in_utes_method1 = pd.read_parquet(path_found_method1)
df_not_in_utes_method2 = pd.read_parquet(path_found_method2)
df_utes = pd.read_parquet(path_utes)

df_not_in_utes_method1["utes_length"] = df_not_in_utes_method1["utes"].apply(
    len)
df_not_in_utes_method2["utes_length"] = df_not_in_utes_method2["utes"].apply(
    len)


filtered_method1 = df_not_in_utes_method1[(
    df_not_in_utes_method1.utes_length > 0)]
filtered_method2 = df_not_in_utes_method2[(
    df_not_in_utes_method2.utes_length > 0)]

utes_found_method1 = [ute for el in filtered_method1.utes.values.tolist()
                      for ute in el]
utes_matching_method1 = (
    len(list(set(utes_found_method1))) / len(df_utes)) * 100
print(utes_matching_method1)

utes_found_method2 = [ute for el in filtered_method2.utes.values.tolist()
                      for ute in el]
utes_matching_method2 = (
    len(list(set(utes_found_method2))) / len(df_utes)) * 100
print(utes_matching_method2)

show = 10

coverage_results = [
    ["Method 1", utes_matching_method1],
    ["Method 2", utes_matching_method2]
]
print("-- Coverage Results:")
print(tabulate(coverage_results, headers=[
      "Method", "Coverage (%)"], tablefmt="mixed_grid"))

table_filtered_method1 = filtered_method1.head(show)[["Name", "utes"]]
table_filtered_method2 = filtered_method2.head(show)[["Name", "utes"]]

sample_results_method1 = [
    (table_filtered_method1.iloc[i]["Name"], "\n".join(table_filtered_method1.iloc[i]["utes"])) for i in range(show)
]

sample_results_method2 = [
    (table_filtered_method2.iloc[i]["Name"], "\n".join(table_filtered_method2.iloc[i]["utes"])) for i in range(show)
]

print("-- Some examples using method 1:")
print(tabulate(sample_results_method1, headers=[
      "Company Names", "Utes"], tablefmt="mixed_grid"))

print("-- Some examples using method 2:")
print(tabulate(sample_results_method2, headers=[
      "Company Names", "Utes"], tablefmt="mixed_grid"))
