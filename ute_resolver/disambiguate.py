import pathlib
import pandas as pd
import re
from fuzzywuzzy import fuzz


def remove_substring(string, substring):
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


def eliminate_patterns(text):
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


def remove_first_substring(text, substring):
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


def extract_difference(str1, str2):
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


def get_splits_additional_rules(ute_rem, split_rules):
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


def split_names(row):

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


def get_company_utes_from_splits(row, df_utes):

    thr = 95
    utes = [
        df_utes.FullName.values[el]
        for el in range(len(df_utes))
        for comp in df_utes.split_names.values[el]
        if fuzz.ratio(row.FullName, comp) > thr
    ]

    return utes


if __name__ == "__main__":

    # Read in the data
    df_company = pd.read_parquet(pathlib.Path("../data/company_info.parquet"))
    df_utes = pd.read_parquet(pathlib.Path("../data/utes.parquet"))

    # Create a new dataframe with all the company names that are not in the utes
    df_not_in_utes = df_company[~df_company['FullName'].isin(
        df_utes['FullName'])]

    # Split names
    df_utes['split_names'] = df_utes['FullName'].apply(split_names)
    df_utes['utes_length'] = df_utes.split_names.apply(len)

    # Get utes for companies
    df_not_in_utes["utes"] = df_not_in_utes.apply(
        lambda row: get_company_utes_from_splits(row, df_utes), axis=1)
    df_not_in_utes['utes_length'] = df_not_in_utes.utes.apply(len)
    df_not_in_utes[(df_not_in_utes.utes_length > 0)]

    df_not_in_utes.to_parquet("data/df_not_in_utes_enriched1.parquet")
