import json
from collections import Counter
from itertools import chain
from pathlib import Path
from datetime import datetime
#from langdetect import detect

import numpy as np
import pandas as pd
import regex, re
#import unidecode
from src.utils.utils import evaluate_cell, fill_na, unify_colname, date_format

def crear_indice_unico(row):
    # Primeros 50 caracteres de 'title'
    inicio_title = row['title'][:50] 
    # ContractFolderStatus.ContractFolderID
    contract_folder_id = row['ContractFolderStatus.ContractFolderID']
    # ContractFolderStatus.ProcurementProject.RealizedLocation.CountrySubentityCode
    country_subentity_code = row['ContractFolderStatus.ProcurementProject.RealizedLocation.CountrySubentityCode']
    nom = row['ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name'][:50]
    # Concatenación de los componentes con el símbolo '&'
    indice_unico = f"{inicio_title}&{contract_folder_id}&{country_subentity_code}&{nom}"
    
    return indice_unico


def crear_indice_unico_gencat(row):
    # Primeros 50 caracteres de 'objecte_contracte'
    inicio_objecte_contracte = row['objecte_contracte'][:50] if pd.notnull(row['objecte_contracte']) else ""
    # 'codi_expedient' completo
    codi_expedient = row['codi_expedient'] 
    # 'codi_nuts' para el componente final del índice
    codi_nuts = row['codi_nuts']
    # 'nom_organ' para el nombre
    nom_organ = row['nom_organ'][:50] if pd.notnull(row['nom_organ']) else ""
    
    # Concatenación de los componentes con el símbolo '&'
    indice_unico = f"{inicio_objecte_contracte}&{codi_expedient}&{codi_nuts}&{nom_organ}"
    
    return indice_unico

#La función sirve para sustituir las columnas a nan por la información de la gencat y dejarlo formateado al formarto PLACE
def remplazar_nan_por_denominacion_con_array(fila, columna_origen, columna_reemplazo):
    # Extraemos el valor actual para verificar si es un array que contiene 'nan'
    valor_actual = fila[columna_origen]
    # Verificamos si el valor es una instancia de una cadena que representa un array con 'nan'
    if isinstance(valor_actual, np.ndarray) and valor_actual.size == 1 and valor_actual[0] == 'nan':
        # Creamos un nuevo array de NumPy con el valor de la columna de reemplazo
        nuevo_valor = np.array([fila[columna_reemplazo]], dtype=object)
        return nuevo_valor
    elif pd.isna(valor_actual):  # Manejamos NaN o valores nulos
        nuevo_valor = np.array([fila[columna_reemplazo]], dtype=object)
        return nuevo_valor
    else:
        # Si el valor actual no necesita ser reemplazado, lo devolvemos tal cual
        return valor_actual

if __name__ == "__main__":
    # Carga de datos
    dir_df_out = Path("/export/usuarios_ml4ds/cggamella/NP-Company-Process/data/DESCARGAS_ENTREGABLES/outsiders.parquet")
    df_out = pd.read_parquet(dir_df_out)
    
    dir_df_gencat = Path("/export/usuarios_ml4ds/cggamella/NP-Company-Process/data/Contractaci__p_blica_a_Catalunya__publicacions_a_la_Plataforma_de_serveis_de_contractaci__p_blica_20240320.csv")
    df_gencat = pd.read_csv(dir_df_gencat)
    
    pd.set_option('display.max_columns', None)
    
    # Aplica la función a cada columna en el DataFrame MultiIndex
    df_out.columns = [unify_colname(col) for col in df_out.columns]
    
    # Aplicamos la función a cada fila del DataFrame para crear el índice único
    df_out['indice_unico'] = df_out.apply(crear_indice_unico, axis=1)
    df_out['indice_unico'] = df_out['indice_unico'].str.lower()
    
    # Aplicamos la función a cada fila del DataFrame df_gencat para crear el índice único
    df_gencat['indice_unico'] = df_gencat.apply(crear_indice_unico_gencat, axis=1)
    df_gencat['indice_unico'] = df_gencat['indice_unico'].str.lower()
    
    columnas_gencat = ['indice_unico', 'codi_cpv', 'lloc_execucio', 'codi_organ', 'codi_unitat', 'nom_unitat', 'codi_ine10', 'codi_dir3', 'identificacio_adjudicatari', 'denominacio_adjudicatari']
    df_coincidencias = pd.merge(df_out, df_gencat[columnas_gencat], on='indice_unico', how='inner')
    
    # Ahora, para aplicar esta función modificada a las columnas deseadas del DataFrame, 
    # utilizaremos una función lambda dentro de apply para pasar los parámetros adicionales:
    columna_origen = 'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name'
    columna_reemplazo = 'denominacio_adjudicatari'
    df_coincidencias[columna_origen] = df_coincidencias.apply(lambda fila: remplazar_nan_por_denominacion_con_array(fila, columna_origen, columna_reemplazo), axis=1)

    # Para aplicar la misma función a otra pareja de columnas, simplemente cambia los valores de `columna_origen` y `columna_reemplazo`
    columna_origen = 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.ID'
    columna_reemplazo = 'identificacio_adjudicatari'
    df_coincidencias[columna_origen] = df_coincidencias.apply(lambda fila: remplazar_nan_por_denominacion_con_array(fila, columna_origen, columna_reemplazo), axis=1)
    # Guardar resultados finales
    # Eliminar las filas duplicadas basadas en la columna 'indice_unico'
    df_coincidencias_sin_duplicados = df_coincidencias.drop_duplicates(subset=['indice_unico']).reset_index(drop=True)

    # Exportar el DataFrame limpio a un archivo CSV
    df_coincidencias_sin_duplicados.to_csv('datos_cruce_gencat_place.csv', index=False)
    print('Datos guardados en datos_cruce_gencat_place.csv')