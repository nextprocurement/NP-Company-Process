from pathlib import Path

import pandas as pd
import regex

_cwd = Path(__file__).resolve().parent

# Define control digits
dig_control_num = "JABCDEFGHI".lower()
secuenciaLetrasNIF = "TRWAGMYFPDXBNJZSQVHLCKE".lower()
identificadoresDNI = "KLM".lower()
identificadoresNIE = "XYZ".lower()
idNIE2letter = {l: str(n) for n, l in enumerate(identificadoresNIE)}

# Load entity identifiers
id_entidades = pd.read_csv(_cwd.joinpath("data/identificador_entidades.csv"), sep=";")
let2desc = dict(id_entidades[["letra", "desc"]].values)
let2tipo = dict(id_entidades[["letra", "tipo"]].values)

# Load province identifiers
id_provincia = pd.read_csv(_cwd.joinpath("data/identificador_provincia.csv"), sep=";")
id_provincia["identificador"] = id_provincia["identificador"].str.split(",")
dig2prov = dict(id_provincia.explode(column="identificador").values)


def is_valid_dni(dni: str, verbose: bool = False):
    """
    Check whether the DNI (documento nacional de identidad) is valid.
    """
    dni = regex.sub(r"\W", "", dni.lower())
    # Check the length of NIF
    if not len(dni) == 9:
        if verbose:
            print("Error: wrong length.")
        return False
    first_digit, digits, control = dni[0], dni[1:8], dni[8]
    # Check first digit is valid
    if first_digit.isnumeric():
        digits = first_digit + digits
    elif not first_digit in identificadoresDNI:
        if verbose:
            print("Error: wrong first digit.")
        return False
    # Check digits are digit
    if not (len(digits)==7 or len(digits)==8):
        if verbose:
            print("Error: not valid sequence of numbers.")
        return False
    if not regex.match(rf"\d{{{len(digits)}}}", digits):
        if verbose:
            print("Error: not valid sequence of numbers.")
        return False
    # Check all digits are different from 0
    if not int(digits):
        if verbose:
            print("Error: invalid sequence of numbers.")
        return False
    # Check letter is valid
    if not control in secuenciaLetrasNIF:
        if verbose:
            print("Error: invalid control digit.")
        return False
    # Check the letter
    id_letra = int(digits) % 23
    num_letra = secuenciaLetrasNIF[id_letra]
    if not num_letra == control:
        if verbose:
            print("Error: wrong number-letter combination.")
        return False
    return True


def is_valid_nie(nie: str, verbose: bool = False):
    """
    Check whether the NIE (número de identificación de extranjero) is valid.
    """
    nie = regex.sub(r"\W", "", nie.lower())
    # Check the length of NIF
    if not len(nie) == 9:
        if verbose:
            print("Error: wrong length.")
        return False
    letter, digits, control = nie[0], nie[1:8], nie[8]
    # Check letter is valid
    if not letter in identificadoresNIE:
        if verbose:
            print("Error: invalid letter.")
        return False
    else:
        digits = idNIE2letter.get(letter, "") + digits
    # Check digits are digit
    if not regex.match(r"\d{8}", digits):
        if verbose:
            print("Error: invalid sequence of numbers.")
        return False
    # Check all digits are different from 0
    if not int(digits):
        if verbose:
            print("Error: invalid sequence of numbers.")
        return False
    # Check letter is valid
    if not control in secuenciaLetrasNIF:
        if verbose:
            print("Error: invalid control digit.")
        return False
    # Check the letter
    id_letra = int(digits) % 23
    num_letra = secuenciaLetrasNIF[id_letra]
    if not num_letra == control:
        if verbose:
            print("Error: wrong number-letter combination.")
        return False
    return True


def is_valid_cif(cif: str, verbose: bool = False):
    """
    Check whether the CIF (código de identificación fiscal) is valid.
    """
    cif = regex.sub(r"\W", "", cif.lower())
    # Check the length of CIF
    if not len(cif) == 9:
        if verbose:
            print("Error: wrong length.")
        return False
    letter, digits, control = cif[0], cif[1:8], cif[8]
    # Check digits are digit
    if not regex.match(r"\d{7}", digits):
        if verbose:
            print("Error: not valid sequence of numbers.")
        return False
    # Check all digits are different from 0
    if not int(digits):
        if verbose:
            print("Error: invalid sequence of numbers.")
        return False
    # Check the letter
    if not letter in let2tipo:
        if verbose:
            print("Error: invalid letter.")
        return False
    # Check the province
    if digits[:2] not in dig2prov:
        if verbose:
            print("Error: invalid province code.")
        return False

    # Compute the control digit
    sum_A = sum(int(d) for i, d in enumerate(digits) if i % 2)
    sum_B = sum(
        sum(int(x) for x in str(int(d) * 2)) for i, d in enumerate(digits) if not i % 2
    )
    sum_C = sum_A + sum_B
    digit_E = sum_C % 10
    D = 10 - digit_E if digit_E != 0 else 0
    # Check the control digit
    if letter in ["p", "q", "r", "s", "w"] or digits[:2] == "00":
        # Control should be a letter
        if control != dig_control_num[D]:
            if verbose:
                print("Error: control digit MUST be a letter.")
            return False
    elif letter in ["a", "b", "e", "h"]:
        # Control should be a number
        if control != str(D):
            if verbose:
                print("Error: control digit MUST be a number.")
            return False
    else:
        # letter and number are valid
        if control != str(D) and control != dig_control_num[D]:
            if verbose:
                print(
                    f"Error: control digit should be either '{D}' or '{dig_control_num[D]}'."
                )
            return False
    return True


def get_nif_type(nif: str, verbose: bool = False):
    """
    Get type of NIF (CIF, DNI or NIE).
    """
    nif = regex.sub(r"\W", "", nif.lower())
    if is_valid_cif(nif, verbose=verbose):
        return "CIF"
    elif is_valid_dni(nif, verbose=verbose):
        return "DNI"
    elif is_valid_nie(nif, verbose=verbose):
        return "NIE"
    return None


def is_valid_nif(nif: str, verbose: bool = False):
    """
    Check whether the NIF (CIF, DNI or NIE) is valid.
    """
    nif = regex.sub(r"\W", "", nif.lower())
    if get_nif_type(nif, verbose=verbose):
        return True
    return False


def validate_nif(nif: str):
    """
    Obtain the NIF in a valid format, else None.
    """
    nif = regex.sub(r"\W", "", nif.lower())
    if is_valid_nif(nif):
        return nif
    return None


def get_info_from_cif(cif: str, verbose: bool = False):
    """
    Returns province and company type given a valid CIF.
    """
    cif = regex.sub(r"\W", "", cif.lower())
    if not is_valid_cif(cif, verbose):
        return (None, None, None)
    prov = dig2prov.get(cif[1:3])
    tipo = let2tipo.get(cif[0])
    desc = let2desc.get(cif[0])
    return (prov, tipo, desc)
