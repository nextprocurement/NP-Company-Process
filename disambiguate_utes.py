"""
This script offers two distinct approaches for disambiguating UTEs:

Rule-based Fuzzy Matching:
-------------------------
The first approach employs a set of predefined rules to segment the names of UTEs and companies. Utilizing the fuzzywuzzy library, it identifies UTEs that closely resemble the segmented components of company names.

Recursive Substring Fuzzy Matching:
-----------------------------------
The second approach combines recursive substring searches within UTE names with rule-based segmentation of UTE and company names. It then identifies partial matches with punctuation separation after filtering out irrelevant terms. These exclusions include:
- Words containing fewer than three characters.
- Numerical values.
- Common Spanish and Catalan words sourced from "es.txt" and "catalan.txt".
- Frequently occurring Spanish surnames and given names extracted from      
  "apellidos.parquet" and "nombres.parquet" files.
- Common terms derived from the splits of company names with frequencies above
  the 95th percentile, obtained from "filtered_elements.txt". This list is generated using the 'generate_stops.py' script.
For efficient computation, this method requires the use of Spark.

Upon disambiguating UTEs, where each company name corresponds to a list of associated UTEs, the script generates a new DataFrame containing UTEs and their respective associated companies (integrantes).

The second approach method offers a larger coverage compared to the first approach. Despite its broader scope, it may generate false positives.

Author: Lorena Calvo-Bartolomé, Saúl Blanco Fortes and Carlos González Gamella
Date: 15/02/2024
"""
import argparse
import pathlib
import re
import sys
import time

import pandas as pd
from fuzzywuzzy import fuzz

############################################################################
# AUXILIARY FUNCTIONS APPROACH 1
############################################################################


def remove_substring(
    string: str,
    substring: str
) -> str:
    """
    Remove substring from string

    Parameters
    ----------
    string : str
        String to be processed
    substring : str
        Substring to be removed from string

    Returns
    -------
    result : str
        String without the substring
    """
    pattern = re.escape(substring)
    result = re.sub(pattern, " ", string)
    return result


def eliminate_patterns(
    text: str
) -> str:
    """
    Eliminate fixed patterns found in several utes (e.g., "ley" + month, "u.t.e." prefix/suffix, etc.)

    Parameters
    ----------
    text : str
        String to be processed

    Returns
    -------
    result : str
        String without the fixed patterns
    """

    # Patterns to be removed from the inut string
    PATTERNS = [
        r"ley.*?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)",
        r"revista.*?\d+",
        r"expte.*?\d+",
        r"ley.*\d+",
        r"contr.*\d+"
    ]
    combined_pattern = re.compile("|".join(PATTERNS), flags=re.IGNORECASE)

    # Remove patterns
    result = re.sub(combined_pattern, "", text)

    # Remove additional strings
    EXP = ["u.t.e.", "u.t.e", "union temporal de empresas",
           "compromiso de ", "abreviadamente", "compromiso"]
    for substring in EXP:
        result = remove_substring(result, substring)

    return result.strip()


def remove_first_substring(
    text: str,
    substring: str
) -> str:
    """Remove only the first occurrence of a substring from a text.

    Parameters
    ----------
    text : str
        Text to be processed
    substring : str
        Substring to be removed from text

    Returns
    -------
    result : str
        Text without the substring
    """

    index = text.find(substring)
    if index != -1:
        return text[:index] + text[index + len(substring):]
    return text


def extract_difference(
    str1: str,
    str2: str
) -> str:
    """Extract the difference between two strings.

    Parameters
    ----------
    str1 : str
        String to be processed
    str2 : str
        String to be processed

    Returns
    -------
    result : str
        Difference between str1 and str2
    """

    names1 = str1.split()
    names2 = set(str2.split())
    difference = sorted(set(names1) - names2, key=lambda x: names1.index(x))
    result = ' '.join(difference)

    return result


def get_splits_additional_rules(
    ute_rem: str,
    split_rules: list
) -> list:
    """Splits a given string (ute_rem) based on set of split_rules and returns a list of split names.

    Parameters
    ----------
    ute_rem : str
        The input string to be split using additional split rules.
    split_rules : list
        A list of split rules to be applied on the input string.

    Returns
    -------
    split_names : list
        A list of strings resulting from splitting the input string based on the given rules.
    """

    # Filter out only the relevant split rules that exist in ute_rem
    split_rules = [rule for rule in split_rules if rule in ute_rem]

    if len(split_rules) == 1:
        # If only one rule is found, split ute_rem using that rule and strip any leading/trailing whitespaces
        split_names = [name.strip() for name in ute_rem.split(split_rules[0])]
    else:
        # If multiple split rules are found, sort them based on their first occurrence in the ute_rem
        split_rules.sort(key=lambda rule: ute_rem.index(
            rule) if rule in ute_rem else len(ute_rem))

        # Create a list of split names by applying each split rule on the remaining ute_rem and stripping leading whitespaces
        split_names = []
        for i, split_rule in enumerate(split_rules):
            found = ute_rem.split(split_rule)[0].strip()
            split_names.append(found)
            ute_rem = remove_substring(ute_rem, found)
            if i == len(split_rules)-1:
                split_names.append(ute_rem)

    return split_names


def split_ute(
    row: str
) -> list:
    """Split a given UTE (row) into multiple strings based on the presence of certain patterns and rules.
    """

    # Remove fixed patterns found in several utes (e.g., "ley" + month, "u.t.e." prefix/suffix, etc.)
    ute_rem = eliminate_patterns(row)

    # Look for utes in the form "letter - number word" or "word letter - number". If so, we keep it as it is
    if re.search(r"([a-zA-Z]) - (\d+) (\S+)", ute_rem) or re.search(r"(\b\w+\b) ([a-zA-Z]) - (\d+)", ute_rem):
        return [ute_rem.strip()]

    # Possible split rules
    look_first_split_rules = \
        [
            "s.l.p.", "s.l.p", "s.l.l.", "s.a.u.", "s.l.u.",
            "s.l.u", "s.l.u,", "slu", "s.l", "c.o.o.p.", "s.a",
            "sl.", "sccl", "s.coop.pequeña"
        ]
    split_rules = [el for el in df_company.CompanyType.unique().tolist(
    ) if el != None and el != 'u.t.e.'] + look_first_split_rules
    additional_split_rules = ["-", "_", ",", "+"]

    # Sort according to size and scape characters in split_rules
    split_rules = sorted(split_rules, key=len, reverse=True)
    scaped_split_rules = [rule.replace('.', '\.').replace(
        ' ', '\s') for rule in split_rules]

    # Find rules
    found_rules = []
    aux = ute_rem
    for rule, escaped_rule in zip(split_rules, scaped_split_rules):
        if re.search(rf'{escaped_rule}', aux):
            occurrences = len(re.findall(rf'{escaped_rule}', aux))
            for _ in range(occurrences):
                found_rules.append(rule)
                aux = remove_first_substring(aux, rule)

    # Check if there are additional rules
    has_additional_rule = [
        True if rule in ute_rem else False for rule in additional_split_rules]

    # If there are found rules according to ute type
    if len(found_rules) > 0:

        # Remove additional_split_rules followed by an ute split_rule
        for element in split_rules:
            for subelement in additional_split_rules:
                pattern = f'{subelement}{element}'
                if pattern in ute_rem:
                    ute_rem = ute_rem.replace(pattern, f' {element}')

        # Check if there are still additional rules
        has_additional_rule = [
            True if rule in ute_rem else False for rule in additional_split_rules]

        # Order according to appearance in ute_rem
        found_rules.sort(key=lambda rule: ute_rem.index(
            rule) if rule in ute_rem else len(ute_rem))

        # If there is only one rule and it is at the end of the string
        if len(found_rules) == 1 and ute_rem.endswith(found_rules[0]):
            # If there are no additional rules
            if not any(has_additional_rule):
                # If the rule is "y" we split the string in two; otherwise, we don't split the string
                if "y" in ute_rem:
                    split_names = get_splits_additional_rules(ute_rem, ["y"])
                else:
                    split_names = [ute_rem.strip()]
            else:
                # If there are additional rules, we split the string according to them
                split_names = get_splits_additional_rules(
                    ute_rem, additional_split_rules)

        else:
            if not any(has_additional_rule):
                split_names = []
                for i, split_rule in enumerate(found_rules):
                    found = ute_rem.split(split_rule)[
                        0].strip() + " " + split_rule
                    split_names.append(found)
                    ute_rem = remove_substring(ute_rem, found)
                    if i == len(split_rules)-1:
                        split_names.append(ute_rem)
            else:
                new_split_names = []
                split_names = get_splits_additional_rules(
                    ute_rem, additional_split_rules)
                for el in split_names:
                    go = [True if rule in el else False for rule in found_rules]
                    if any(go):
                        for i, split_rule in enumerate(found_rules):
                            found = el.split(split_rule)[
                                0].strip() + " " + split_rule
                            new_split_names.append(found)
                            el = remove_substring(el, found)

                            if i == len(found_rules) - 1:
                                new_split_names.append(el)
                    else:
                        new_split_names.append(el)
                split_names = new_split_names

    elif True in has_additional_rule:
        split_names = get_splits_additional_rules(
            ute_rem, additional_split_rules + [" y "])

    elif re.compile(r'\s+y\s+').search(ute_rem):
        split_names = get_splits_additional_rules(ute_rem, [" y "])

    else:

        substrings = [" ".join(ute_rem.split()[i:j]) for i in range(
            len(ute_rem.split())) for j in range(i + 1, len(ute_rem.split()) + 1)]

        split_names = [
            el for el in substrings if el in df_not_in_utes.FullName.values.tolist()]

        if len(split_names) == 1:
            split_names += [extract_difference(ute_rem, split_names[0])]

        elif len(split_names) == 0:
            split_names = [ute_rem.strip()]

    # Cleaning errors after splitting
    split_names = [name.strip("-_ ,,+y").strip()
                   for name in split_names if len(name.strip()) > 1]

    return split_names


def get_company_utes_from_splits(
    row: pd.Series,
    df_utes: pd.DataFrame,
    thr: int = 95
) -> list:
    """
    Get a list of company UTEs based on fuzzy matching with split names.

    Parameters
    ----------
    row: pd.Series
        The row containing the company information. 
    df_utes: pd.DataFrame
        The DataFrame containing the UTE information. 
    thr: int, optional
        The threshold for fuzzy matching. Defaults to 95.

    Returns
    -------
        list: A list of company UTEs.
    """

    utes = [
        df_utes.FullName.values[el]
        for el in range(len(df_utes))
        for comp in df_utes.split_names.values[el]
        if fuzz.ratio(row.FullName, comp) > thr
    ]

    return utes

############################################################################
# AUXILIARY FUNCTIONS APPROACH 2
############################################################################


def find_substring_similar(
    main_string: str,
    substring: str,
    substring_splits: list,
    threshold: int = 95
) -> bool:
    """
    Given the name of an UTE (main_string), that of a company (substring), and the "splits" of the company name (i.e. a list of the words that compose the company name, after filtering out some terms), it first try to find the substring within the main_string with a similarity ratio larger than a threshold. If it does not find it, it checks for partial matches with some punctuation separation.

    Parameters
    ----------
    main_string : str
        The name of the UTE.
    substring : str
        The name of the company.
    substring_splits : list
        The "splits" of the company name, after filtering out some terms.
    threshold : int
        The threshold for the similarity ratio. The default is 95.

    Returns
    -------
    bool
        Whether the substring is found within the main_string with a similarity ratio larger than a threshold or not.
    """

    ############################################################################
    # Check for exact matches
    ############################################################################
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

    ############################################################################
    # Check for partial matches with some punctuation separation
    ############################################################################
    if re.search(r'[.,!?;:\s()-]', main_string):
        main_string_split = set(
            re.split(r'([.,!?;:\s()-])', main_string.lower()))
        # if any(len(split) > 3 and split in main_string_split for split in substring_splits):
        substring_set = set(substring_splits)

        if len(main_string_split.intersection(substring_set)) >= 1:
            return True


def get_splits(
    name: str
) -> list:
    """Split the name of a company into its parts, after filtering out some terms.

    The terms that are filtered out are:
    - Words with less than 3 characters.
    - Numbers.
    - Common words in Spanish and Catalan (extracted from the files "es.txt" and "catalan.txt").
    - Common last names and names in Spain (extracted from the files "apellidos.parquet" and "nombres.parquet").
    - Common words obtained from the "splits" of the company names that have a frequency larger than the 95th percentile of the frequency distribution of the "splits" of the company names (extracted from the file "filtered_elements.txt").

    Parameters
    ----------
    name : str
        The name of the company.

    Returns
    -------
    list
        The parts of the name of the company, after filtering out some terms.
    """
    if re_pattern.search(name):
        name_parts = re_pattern.split(name.lower())
        # Remove empty strings, separators, and duplicates
        name_parts = [part.strip(',').strip()
                      for part in name_parts
                      if part.strip()
                      and len(part) > 3
                      and not part.isdigit()
                      and not part in SEPS
                      and not part in (UNIQUE_NAMES_APELLIDOS + COMMON_SPANISH + COMMON_CATALAN + SPLITS_STOPS)]
        return name_parts if len(name_parts) > 0 else []
    return []


def remove_punctuation(
    name: str
) -> str:
    """Remove punctuation and accents from a string.

    Parameters
    ----------
    name : str
        The name of the company.

    Returns
    -------
    str
        The name of the company without punctuation and accents.
    """
    return name.translate(TRANS_TABLE)

############################################################################
# COMMON AUXILIARY FUNCTIONS
############################################################################
def get_utes_des(
    ruta_utes_file: str,
    ruta_datos: str,
    ruta_file_escritura: str
) -> None:

    # Convertir las rutas de string a objetos Path
    ruta_utes_file = pathlib.Path(ruta_utes_file)
    ruta_datos = pathlib.Path(ruta_datos)
    ruta_file_escritura = pathlib.Path(ruta_file_escritura)

    # Leer archivos Parquet
    df_utes = pd.read_parquet(ruta_utes_file)
    df = pd.read_parquet(ruta_datos)

    # Procesar datos
    df["utes_length"] = df.utes.apply(len)
    df_con_utes = df[df.utes_length > 0]  # Coger lista de compañías con utes
    all_comp = df_con_utes.FullName.values.tolist()
    all_utes = df_con_utes.utes.values.tolist()

    # Aplanar la lista de utes y eliminar duplicados
    def flatten(xss):
        return [x for xs in xss for x in xs]

    all_utes_d = list(set(flatten(all_utes)))

    # Crear un diccionario para asociar utes con compañías
    utes_dict = {ute: [] for ute in all_utes_d}

    for comp, utes in zip(all_comp, all_utes):
        for ute in utes:
            utes_dict[ute].append(comp)

    # Leer el DataFrame de company_info y actualizarlo
    # La función map busca cada valor de 'FullName' en las claves del diccionario (utes_dict) y asignará el valor correspondiente
    df_utes['integrantes'] = df_utes['FullName'].map(utes_dict)

    # Escribir el DataFrame actualizado en el archivo Parquet de salida
    df_utes.to_parquet(ruta_file_escritura)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--path_data", type=str,
                        help="Path to the data folder.",
                        default="/export/usuarios_ml4ds/lbartolome/NextProcurement/NP-Company-Process/data/tablas_nuevas")
    parser.add_argument("--path_aux", type=str,
                        help="Path to the folder with auxiliary data.",
                        default="/export/usuarios_ml4ds/lbartolome/NextProcurement/NP-Company-Process/ute_resolver/metalists")
    parser.add_argument("--approach", type=int,
                        help="Disambiguation approach to use.", default=2)

    args = parser.parse_args()

    # Define paths to data
    path_data = pathlib.Path(args.path_data)
    path_companies = path_data.joinpath("company_info.parquet")
    path_utes = path_data.joinpath("utes.parquet")

    df_company = pd.read_parquet(path_companies)
    SEPS_UTES = (df_company.CompanyType.unique().tolist() +
                 ["s.l.p.", "s.l.p", "s.l.l.", "s.a.u.", "s.l.u.", "s.l.u", "s.l.u,", "slu", "s.l", "c.o.o.p.", "s.a", "sl.", "sccl", "s.coop.pequeña"] +
                 ["sl", "slu", "s.", "scclp"])[1:]

    OTHERS = ["-", "_", ",", "+", ".", ".,"]
    SEPS = SEPS_UTES + OTHERS

    start_time = time.time()

    if not args.approach or args.approach < 1 or args.approach > 2:
        sys.exit("-- -- The approach must be 1 or 2")

    # Approach 1
    elif args.approach == 1:

        # Read in the data
        df_utes = pd.read_parquet(path_utes)

        # Create a new dataframe with all the company names that are not in the utes
        df_not_in_utes = df_company[~df_company['FullName'].isin(
            df_utes['FullName'])]

        # Split names
        df_utes['split_names'] = df_utes['FullName'].apply(split_ute)
        df_utes['utes_length'] = df_utes.split_names.apply(len)

        # Get utes for companies
        df_not_in_utes["utes"] = df_not_in_utes.apply(
            lambda row: get_company_utes_from_splits(row, df_utes), axis=1)
        df_not_in_utes['utes_length'] = df_not_in_utes.utes.apply(len)
        df_not_in_utes[(df_not_in_utes.utes_length > 0)]

        path_save = path_data.joinpath("df_not_in_utes_approach1.parquet")
        df_not_in_utes.to_parquet(path_save.as_posix())

    # Approach 2
    elif args.approach == 2:

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, explode, split, udf
        from pyspark.sql.types import ArrayType, StringType

        spark = SparkSession\
            .builder\
            .appName("UTEs")\
            .getOrCreate()

        # GLOBAL VARIABLES
        re_pattern = re.compile(r'([.,!?;:\s()-])')
        TRANS_TABLE = str.maketrans('áéíóúÁÉÍÓÚòÒ', 'aeiouAEIOUoO')

        ########################################################################
        # Read data as pyarrow dataframes, normalize to remove punctuation, create 'df_not_in_utes' and a list with the UTE's unique names
        ########################################################################
        # Read data
        df_company = spark.read.parquet(
            f"file://{path_companies}")  # .sample(withReplacement=False, fraction=0.1, seed=42)
        df_utes = spark.read.parquet(
            f"file://{path_utes}")  # .sample(withReplacement=False, fraction=0.1, seed=42)

        # Create a new dataframe with all the company names that are not in the utes
        full_names_utes = df_utes.select(
            'FullName').rdd.flatMap(lambda x: x).collect()
        df_not_in_utes = df_company.filter(
            ~col('FullName').isin(full_names_utes))

        # Normalize to remove punctuation
        remove_punctuation_udf = udf(remove_punctuation, StringType())
        df_not_in_utes = df_not_in_utes.withColumn(
            "Name_norm", remove_punctuation_udf("Name"))
        df_utes = df_utes.withColumn(
            "FullName_norm", remove_punctuation_udf("FullName"))

        ########################################################################
        # Create lists of common names/last names in Spain and broadcast them
        ########################################################################
        path_aux = pathlib.Path(args.path_aux)
        path_apellidos = path_aux / ("apellidos.parquet")
        path_nombres = path_aux / ("nombres.parquet")

        df_appellidos = spark.read.parquet(f"file://{path_apellidos}")
        df_nombres = spark.read.parquet(f"file://{path_nombres}")

        # Apply transformation to divide composed names into new items
        df_split = df_appellidos.withColumn(
            "palabras", split(df_appellidos["apellidos"], " "))
        df_appellidos = df_split.select(
            explode(df_split["palabras"]).alias("apellidos"))

        df_split = df_nombres.withColumn(
            "palabras", split(df_nombres["nombres"], " "))
        df_nombres = df_split.select(
            explode(df_split["palabras"]).alias("nombres"))

        # Collect the unique names and last names and broadcast them
        unique_apellidos = df_appellidos.select(
            'apellidos').distinct().rdd.flatMap(lambda x: x).collect()
        unique_names = df_nombres.select(
            'nombres').distinct().rdd.flatMap(lambda x: x).collect()
        UNIQUE_NAMES_APELLIDOS = list(set(unique_names + unique_apellidos))
        UNIQUE_NAMES_APELLIDOS.sort()

        ########################################################################
        # Create list of common spanish and catalan words
        ########################################################################
        with open(path_aux.joinpath("es.txt")) as file:
            COMMON_SPANISH = file.readlines()
            COMMON_SPANISH = [line.rstrip() for line in COMMON_SPANISH]
        with open(path_aux.joinpath("catalan.txt")) as file:
            COMMON_CATALAN = file.readlines()
            COMMON_CATALAN = [line.rstrip() for line in COMMON_CATALAN]

        ########################################################################
        # We also laod a list of words generated from the "splits" of the company names obtained from a previous execution of this script that have a frequency larger than the 95th percentile of the frequency distribution of the "splits" of the company names
        ########################################################################
        with open(path_aux.joinpath("filtered_elements.txt")) as file:
            SPLITS_STOPS = file.readlines()
            SPLITS_STOPS = [line.rstrip() for line in SPLITS_STOPS]

        # UDF to split the names and apply UDF to DataFrame
        split_udf = udf(get_splits, ArrayType(StringType()))
        df_not_in_utes = df_not_in_utes.withColumn(
            "splits", split_udf("Name_norm"))

        # Extract the distinct FullName_norm values from df_utes, collect them as a list and broadcast
        unique_names = df_utes.select(
            'FullName_norm').distinct().rdd.flatMap(lambda x: x).collect()
        broadcast_unique_names = spark.sparkContext.broadcast(unique_names)

        # UDF that iterates over the broadcasted list of unique names and finds matches for each name in df_not_in_utes.
        @udf(returnType=ArrayType(StringType()))
        def get_company_utes(name, splits):
            utes = [fullName for fullName in broadcast_unique_names.value if find_substring_similar(fullName, name, splits)]
            return list(set(utes))
        # si encontramos el nombre de la empresa (name) en el ute (fullName)
        # Use the UDF to add a new column to df_not_in_utes
        df_not_in_utes = df_not_in_utes.withColumn(
            "utes", get_company_utes("Name_norm", "splits"))

        # Save to file
        print("--- Saving of file starts...")
        path_save = path_data.joinpath("df_not_in_utes_approach2.parquet")
        df_not_in_utes.coalesce(1000).write.parquet(
            f"file://{path_save}", mode="overwrite")
        print("--- Saving of file finished!!...")

    end_time = time.time()

    execution_time = end_time - start_time
    print(
        f"-- Execution time for Approach {str(args.approach)}: {execution_time}")

    # Get "utes_des", i.e. a DataFrame with the UTEs and their associated companies (integrantes).
    print(f"-- Getting 'utes_des'...")
    path_new_save = path_data.joinpath("utes_des.parquet")
    get_utes_des(
        path_utes.as_posix(),
        path_save.as_posix(),
        path_new_save.as_posix()
    )
