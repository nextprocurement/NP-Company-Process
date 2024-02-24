import pandas as pd
import pathlib

def procesar_datos_utes(ruta_utes_file: str, ruta_datos: str, ruta_file_escritura: str) -> None:
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

# Ejemplo de cómo llamar a la función
if __name__ == "__main__":
    ruta_utes = "/export/usuarios_ml4ds/cggamella/sproc/Compartido_Lorena/utes_actualizados.parquet"
    ruta_datos = "../data/utes_spark.parquet"
    ruta_file_escritura = "../data/utes_des.parquet"

    # Llamar a la función con las rutas de los archivos
    procesar_datos_utes(ruta_utes, ruta_datos, ruta_file_escritura)