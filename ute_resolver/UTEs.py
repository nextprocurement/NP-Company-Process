import re
import pathlib
import pandas as pd
from fuzzywuzzy import fuzz
import concurrent.futures
import time

#######################
# AUXILIARY FUNCTIONS #
#######################

"""
def encontrar_substring_similar(main_string, substring, threshold=95):
    regex_pattern = r'\b' + re.escape(substring) + r'\b'
    regex = re.compile(regex_pattern)

    # Precompute length outside the loop
    len_substring = len(substring)
    for i in range(len(main_string) - len_substring + 1):
        sub = main_string[i:i + len_substring]

        # Check if substring matches main_string
        if not regex.search(main_string):
            continue

        # Calculate similarity
        similitud = fuzz.ratio(sub, substring)
        if similitud >= threshold:
            return True

    return False
"""

def encontrar_substring_similar(main_string, substring, threshold=95):
    regex_pattern = r'\b' + re.escape(substring) + r'\b'
    regex = re.compile(regex_pattern)

    len_substring = len(substring)
    for match in regex.finditer(main_string):
        start_index = match.start()
        end_index = match.end()

        # Calculate similarity only for substrings around the match
        for i in range(start_index, end_index - len_substring + 1):
            sub = main_string[i:i + len_substring]
            similitud = fuzz.ratio(sub, substring)
            if similitud >= threshold:
                return True

    return False

def flatten_comprehension(matrix):
    return [item for row in matrix for item in row]

def work(company_name, utes):
    salida = []
    for ute in utes:
        salida.append ({
                        'ute':ute,
                        'company_name':company_name,
                        'ratio':encontrar_substring_similar(ute, company_name)
                })
    return salida

if __name__ == "__main__":
    
    spark = False
    
    # Define paths to data
    path_data = pathlib.Path("../data")
    path_companies = path_data.joinpath("company_info.parquet")
    path_utes = path_data.joinpath("utes.parquet")
    
    if spark:
        
        from pyspark.sql.functions import udf, col
        from pyspark.sql.types import BooleanType, ArrayType
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StringType

        spark = SparkSession\
            .builder\
            .appName("UTEs")\
            .getOrCreate()

        # Read data as pyarrow dataframes
        df_company = spark.read.parquet(f"file://{path_companies}")
        df_utes = spark.read.parquet(f"file://{path_utes}")

        # Create a dataframe with the companies that are not utes
        full_names_utes = df_utes.select(
            'FullName').rdd.flatMap(lambda x: x).collect()
        df_not_in_utes = df_company.filter(~col('FullName').isin(full_names_utes))

        # Extract the distinct FullName values from df_utes and collect them as a list.
        unique_names = df_utes.select(
            'FullName').distinct().rdd.flatMap(lambda x: x).collect()

        # Broadcast the list of unique names
        broadcast_unique_names = spark.sparkContext.broadcast(unique_names)

        # UDF that iterates over the broadcasted list of unique names and finds matches for each name in df_not_in_utes.
        @udf(returnType=ArrayType(StringType()))
        def get_company_utes(name):
            return [fullName for fullName in broadcast_unique_names.value if encontrar_substring_similar(fullName, name)]

        # Use the UDF to add a new column to df_not_in_utes
        df_not_in_utes = df_not_in_utes.withColumn(
            "utes", get_company_utes("Name"))

        # Save to file
        print("--- Saving of file starts...")
        path_save = path_data.joinpath("utes_spark.parquet")
        df_not_in_utes.coalesce(1000).write.parquet(
            f"file://{path_save}", mode="overwrite")
        print("--- Saving of file finished!!...")
        
    else:
        
        # Read data
        df_company = pd.read_parquet(path_companies)
        df_utes = pd.read_parquet(path_utes)
        
        print("Data read")
        
        # Create a new dataframe with all the company names that are not in the utes
        df_not_in_utes = df_company[~df_company['FullName'].isin(df_utes['FullName'])]
        
        print("Dataframes created")
        
        # Extract the distinct FullName values from df_utes and collect them as a list.
        unique_names = df_utes.FullName.unique().tolist()
        
        print(f" {len(unique_names)} unique names extracted")
        
        # Extract the distinct Name values from df_not_in_utes and collect them as a list
        unique_not_in_utes = df_not_in_utes.Name.values.tolist()
        
        print(f" {len(unique_not_in_utes)} unique names not in utes extracted")
        
        start_time = time.time()
        
        print("Start of parallel execution")

        with concurrent.futures.ProcessPoolExecutor(max_workers=40) as executor:
            futures = [executor.submit(work, word, unique_names) for word in unique_not_in_utes]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
            executor.shutdown(wait=True, cancel_futures=True)
            
        results = flatten_comprehension(results)
        
        all_utes_per_company = []
        for company_name in unique_not_in_utes:
            utes_for_company = [dato['ute'] for dato in results if dato['company_name'] == company_name and dato['ratio']]
            all_utes_per_company.append(utes_for_company)
                
        df_not_in_utes["utes"] = all_utes_per_company 

        end_time = time.time()

        execution_time = end_time - start_time
        print("Execution time paralelo:", execution_time)
        
        import pdb; pdb.set_trace()
        
