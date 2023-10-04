import regex
from unidecode import unidecode

from .utils import replace_company_types


def clean_text(text: str):
    """
    Clean text to remove extra characters, spaces, etc.
    """
    text = text.lower()
    # remove special chars
    text = regex.sub(r"\p{P}(?<![,.-])", " ", text).replace("-", " - ")
    # strip chars
    text = regex.sub(r"^[\s\p{P}]*|[\s\p{P}]*(?<![.])$", "", text)
    # remove extra spaces
    text = regex.sub(r"\s{2,}", " ", text)
    return text


def clean_company_type(company_name: str, remove_type: bool = False):
    """
    Replace the company type if present in a text in any given format
    (e.g.: "s.l.", "sl", "s. l.") into a standard form ("s.l.")
    or remove it if `remove_type`=`True`.
    """
    if not company_name:
        return None
    company_name = company_name.lower()
    company_name = replace_company_types(company_name, remove_type=remove_type)
    # company_name = regex.sub(r"[\s]+", " ", company_name)
    # company_name = company_name.strip("-, ")
    company_name = clean_text(company_name)
    return company_name


def normalize_company_name(company_name: str):
    """
    Remove all non alpha characters, diacritics and company types from name.
    """
    if not company_name:
        return None
    company_name = company_name.lower()
    company_name = unidecode(company_name)
    company_name = regex.sub(r"\W", "", company_name)
    company_name = company_name.strip()
    return company_name
