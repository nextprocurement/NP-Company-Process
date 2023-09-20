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

# Define error types
VALID = 1
ERROR_LENGTH = -1
ERROR_FIRST_DIGIT = -2
ERROR_DIGIT_SEQUENCE = -3
ERROR_CONTROL_DIGIT = -4
ERROR_PROVINCE_CODE = -5


def is_valid_dni(dni: str):
    """
    Check whether the DNI (documento nacional de identidad) is valid.
    """
    dni = regex.sub(r"\W", "", dni.lower())

    # Check the length of NIF
    if not len(dni) == 9:
        return ERROR_LENGTH

    first_digit, digits, control = dni[0], dni[1:8], dni[8]

    # Check first digit is valid
    if first_digit.isnumeric():
        digits = first_digit + digits
    elif not first_digit in identificadoresDNI:
        return ERROR_FIRST_DIGIT

    # Check digits are digit and different from 0
    if not (len(digits) == 7 or len(digits) == 8):
        return ERROR_DIGIT_SEQUENCE
    if not regex.match(rf"\d{{{len(digits)}}}", digits):
        return ERROR_DIGIT_SEQUENCE
    if not int(digits):
        return ERROR_DIGIT_SEQUENCE

    # Check control letter
    if not control in secuenciaLetrasNIF:
        return ERROR_CONTROL_DIGIT
    id_letra = int(digits) % 23
    num_letra = secuenciaLetrasNIF[id_letra]
    if not num_letra == control:
        return ERROR_CONTROL_DIGIT

    return VALID


def is_valid_nie(nie: str):
    """
    Check whether the NIE (número de identificación de extranjero) is valid.
    """
    nie = regex.sub(r"\W", "", nie.lower())
    # Check the length of NIF
    if not len(nie) == 9:
        return ERROR_LENGTH

    letter, digits, control = nie[0], nie[1:8], nie[8]

    # Check letter is valid
    if not letter in identificadoresNIE:
        return ERROR_FIRST_DIGIT
    else:
        digits = idNIE2letter.get(letter, "") + digits

    # Check digits are digit and different from 0
    if not regex.match(r"\d{8}", digits):
        return ERROR_DIGIT_SEQUENCE
    if not int(digits):
        return ERROR_DIGIT_SEQUENCE

    # Check control letter
    if not control in secuenciaLetrasNIF:
        return ERROR_CONTROL_DIGIT
    id_letra = int(digits) % 23
    num_letra = secuenciaLetrasNIF[id_letra]
    if not num_letra == control:
        return ERROR_CONTROL_DIGIT

    return VALID


def is_valid_cif(cif: str):
    """
    Check whether the CIF (código de identificación fiscal) is valid.
    """
    cif = regex.sub(r"\W", "", cif.lower())
    # Check the length of CIF
    if not len(cif) == 9:
        return ERROR_LENGTH

    letter, digits, control = cif[0], cif[1:8], cif[8]

    # Check digits are digit and different from 0
    if not regex.match(r"\d{7}", digits):
        return ERROR_DIGIT_SEQUENCE
    if not int(digits):
        return ERROR_DIGIT_SEQUENCE

    # Check the letter
    if not letter in let2tipo:
        return ERROR_FIRST_DIGIT

    # Check the province
    if digits[:2] not in dig2prov:
        return ERROR_PROVINCE_CODE

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
            return ERROR_CONTROL_DIGIT
    elif letter in ["a", "b", "e", "h"]:
        # Control should be a number
        if control != str(D):
            return ERROR_CONTROL_DIGIT
    else:
        # letter and number are valid
        if control != str(D) and control != dig_control_num[D]:
            return ERROR_CONTROL_DIGIT

    return True


def handle_error(nif, error_code: int, verbose: bool = False):
    """
    Handle the error by printing the corresponding error message.
    """
    error_messages = {
        ERROR_LENGTH: "Error: wrong length.",
        ERROR_FIRST_DIGIT: "Error: wrong first digit.",
        ERROR_DIGIT_SEQUENCE: "Error: invalid sequence of numbers.",
        ERROR_CONTROL_DIGIT: "Error: wrong control digit.",
        ERROR_PROVINCE_CODE: "Error: wrong province code.",
    }

    # Print the error message corresponding to the error code
    if verbose:
        print(error_messages.get(error_code, "Unknown error."))
    if ERROR_LENGTH:
        nif = "0" + nif
        return nif
    elif ERROR_FIRST_DIGIT:
        return None
    elif ERROR_DIGIT_SEQUENCE:
        return None
    elif ERROR_CONTROL_DIGIT:
        return None
    elif ERROR_PROVINCE_CODE:
        return None
    return None


def get_nif_type(nif: str):
    """
    Get type of NIF (CIF, DNI or NIE).
    """
    nif = regex.sub(r"\W", "", nif.lower())
    if is_valid_cif(nif) > 0:
        return "CIF"
    elif is_valid_dni(nif) > 0:
        return "DNI"
    elif is_valid_nie(nif) > 0:
        return "NIE"
    return None


def is_valid_nif(nif: str):
    """
    Check whether the NIF (CIF, DNI or NIE) is valid.
    """
    nif = regex.sub(r"\W", "", nif.lower())
    if get_nif_type(nif):
        return True
    return False


# def validate_nif(nif: str):
#     """
#     Obtain the NIF in a valid format, else None.
#     """
#     nif = regex.sub(r"\W", "", nif.lower())
#     if is_valid_nif(nif):
#         return nif
#     return None


def validate_nif(nif: str, correct=False, verbose=False):
    """
    Obtain the NIF in a valid format.
    If it is wrong and correct is `False`, return None.
    Else try to correct.
    """
    nif = regex.sub(r"\W", "", nif.lower())

    # First check if NIF is valid
    result = get_nif_type(nif)
    if result:
        return nif
    elif not correct:
        return None
    else:
        # If not valid, handle error
        # Check every posibility
        for val in [is_valid_cif, is_valid_dni, is_valid_nie]:
            result = val(nif)
            corrected = handle_error(nif, result, verbose)
            if corrected and val(corrected) > 0:
                return corrected
        return None


def get_info_from_cif(cif: str):
    """
    Returns province and company type given a valid CIF.
    """
    cif = regex.sub(r"\W", "", cif.lower())
    if is_valid_cif(cif) < 0:
        return (None, None, None)
    prov = dig2prov.get(cif[1:3])
    tipo = let2tipo.get(cif[0])
    desc = let2desc.get(cif[0])
    return (prov, tipo, desc)
