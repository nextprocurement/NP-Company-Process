import random
import string
from rapidfuzz import process, fuzz as rfuzz
import re
import time
from fuzzywuzzy import fuzz
import concurrent.futures


def encontrar_substring_similarimproved(main_string, substring, threshold=95):
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



num = 50_000
numSample = num//100

words = [
    "".join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
    for _ in range(num)
]
samples = words[:: len(words) // numSample]   

def flatten_comprehension(matrix):
    return [item for row in matrix for item in row]

def work (word):
    salida = []
    for sample in samples:
        salida.append ({
                        'cadena1':sample,
                        'cadena2':word,
                        'ratio':encontrar_substring_similarimproved (sample, word)
                })
    return salida

if __name__ == "__main__":


    start_time = time.time()

    with concurrent.futures.ProcessPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(work, word) for word in words]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
        executor.shutdown(wait=True, cancel_futures=True)


    end_time = time.time()

    execution_time = end_time - start_time
    print("Execution time paralelo:", execution_time)

    results = flatten_comprehension (results)
    print(results)
    encontrados = [dato for dato in results if dato['ratio']]
    print ("Encontrados:%s" % len(encontrados))


    """
    start_time = time.time()

    resultado3 = []


    for sample in samples:
        for word in words:
            resultado3.append ({
                    'cadena1':sample,
                    'cadena2':word,
                    'ratio':encontrar_substring_similarimproved (sample, word)
            })
    

    end_time = time.time()
    execution_time = end_time - start_time
    print("Execution time secuencial:",execution_time)
    encontrados = [dato for dato in resultado3 if dato['ratio']]
    print ("Encontrados:%s" % len (encontrados))
    """
