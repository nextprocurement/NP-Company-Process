# NP-COMPANY-PROCESS

## Funcionalidad

Este proyecto proporciona notebooks para procesar e identificar licitaciones y empresas, así como para desambiguar las empresas que componen una Unión Temporal de Empresas (UTE).

## Estructura de ficheros

### Datos

El directorio definido con los datos utilizados por la aplicación es por defecto: **_data_**. Dentro de ese directorio se encontrarán otros subdirectorios, principalmente metadata. Por ejemplo:

```bash
data
└───metadata
    ├───insiders.parquet
    ├───outsiders.parquet
    └───minors.parquet
└───company_info.parquet
└───utes.parquet
```

### app/src

Contiene las diversas funcionalidades de la aplicación, divididas en companies, nif_validation y utils.

#### companies

Contiene funciones para identificar el tipo de empresa y unificar su escritura: se establece todo con sus siglas; por ejemplo: _sociedad limitada_, _soc. limitada_, _s. l._ se convierten en _s.l._. Permite también eliminarlo del texto si se desea.

#### nif_validation

Permite comprobar si los distintos identificadores (DNI, NIE y CIF) son correctos, así como identificar cuál se usa y obtener los datos en base a ello.\
Se ha usado para crear estos métodos la información que aparecen en el BOE, principalmente a través de las referencias de <https://es.wikipedia.org/wiki/N%C3%BAmero_de_identificaci%C3%B3n_fiscal>

#### utils

Aquí se encuentran funciones con diversas finalidades, como paralelizar o cargar datos.

Por supuesto, aquí tienes el texto traducido al español manteniendo el formato markdown:

### ute_resolver

Este directorio contiene los scripts que implementan la desambiguación de UTEs. El resultado de este proceso son dos tablas: ``utes_des.parquet`` y ``companies_des.parquet`` que se han generado en base a la información disponible en ``company_info.parquet`` y ``utes.parquet``. El primero contiene las empresas que componen cada UTE. Es importante señalar que puede haber casos de falsos positivos, donde las empresas aparecen erróneamente asociadas con una UTE, así como casos donde faltan empresas (por ejemplo, en una UTE compuesta por 3 empresas, solo se listan 2 en la tabla). Los falsos positivos son especialmente comunes cuando el nombre de una empresa es genérico (por ejemplo, 'José', 'María', etc.). La tabla ``companies_des.parquet`` contiene, para cada empresa, las UTEs en las que el algoritmo sugiere que han participado.

```bash
ute_resolver
├── metalists
│   ├── apellidos.parquet
│   ├── catalan.txt
│   ├── es.txt
│   ├── filtered_elements.txt
│   └── nombres.parquet
├── calculate_coverage.py
├── disambiguate_utes.py
└── generate_stops.py
├── test_zaragoza.ipynb
```

El script ``disambiguate_utes.py`` es el encargado de la generación de los archivos ``utes_des.parquet`` y ``companies_des.parquet`` con los resultados de la desambiguación. Ofrece dos enfoques distintos para la desambiguación de UTEs:

- **Emparejamiento difuso basado en reglas:** El primer enfoque utiliza un conjunto de reglas predefinidas para segmentar los nombres de las UTEs y las empresas. Utilizando la biblioteca fuzzywuzzy, identifica UTEs que se parecen estrechamente a los componentes segmentados de los nombres de las empresas.

- **Emparejamiento difuso de subcadenas recursivas:** El segundo enfoque combina búsquedas de subcadenas recursivas dentro de los nombres de las UTEs con segmentación basada en reglas de los nombres de las UTEs y las empresas. Luego identifica coincidencias parciales con separación de puntuación, después de filtrar términos irrelevantes. Estas exclusiones incluyen:
  - Palabras que contienen menos de tres caracteres.
  - Valores numéricos.
  - Palabras comunes en español y catalán obtenidas de ``es.txt`` y ``catalan.txt``.
  - Apellidos y nombres españoles frecuentes extraídos de los archivos ``apellidos.parquet`` y ``nombres.parquet``.
  - Términos comunes derivados de las divisiones de nombres de empresas con frecuencias por encima del percentil 95, obtenidos de ``filtered_elements.txt.`` Esta lista se genera utilizando el script generate_stops.py.

Para una computación eficiente, este método requiere el uso de Spark.
Tras la desambiguación de las UTEs, cada nombre de empresa se asocia con una lista de UTEs en las que dicha empresa ha participado. Además, el script genera un nuevo dataframe que contiene para cada UTE, sus respectivas empresas asociadas.

El método del segundo enfoque ofrece una cobertura más amplia en comparación con el primero. A pesar de su alcance más amplio, puede generar falsos positivos.

### Notebooks

#### match_tender.ipynb

Este notebook hace un emparejamiento entre las licitaciones de PLACE y las de GENCAT en este caso. El paso a paso del notebook es el siguiente:

1. Cargar los datos de PLACE y GENCAT.
2. Funciones para hacer una limpieza de los dataframes y seleccionar las columnas relevantes.
3. Compilar los datos de PLACE
    - **Get relevant data**
        - Unificar columnas
        - Separar CPVs
        - Limpiar columnas
        - Crear una columna `cpv_div` para usar en el merge
    - **Get data from cat**
        - Obtener potenciales datos de Cataluña
        - Juntarlos por ID y quedarnos únicamente con los que tienen el mismo título
4. Repetir el paso anterior para los datos de GENCAT
5. Hacer una asociación por ID y utilizar solamente ["id_orig", "title", "cpv_div", "index_agg", "index_cat"], donde index son los índices de los dataframes limpios obtenidos en el punto anterior (`use_tend_agg` y `use_tend_cat`). Se hace un counter de títulos y cpvs para quedarnos más tarde con el más común.
6. Repetir la asociación pero utilizando las columnas de `title` y `cpv_div`.
7. Unificar ambos dataframes obteniendo un contador del resto de elementos. Más adelante, este contador servirá para obtener los datos más probables.
8. A partir de ahí ya se pueden hacer búsquedas en un dataframe u otro, rellenar datos, etc.

#### match_companies.ipynb

Este notebook hace un emparejamiento entre empresas. El paso a paso del notebook es el siguiente:

1. Imports y funciones para separar el NIF cuando va incluido en el texto y para limpiar el dataframe.
2. Juntar las distintas fuentes (minors, insiders, outsiders)
3. Filtrar para usar únicamente las empresas que contienen nombre y NIF (se descartan los NaNs)
4. Hay una sección con las empresas de otros países que se puede explorar
5. Limpieza de información (validar NIF, normalizar textos, etc.)
6. Juntar por ID y nombre y determinar si es PYME
7. Separar empresas que tienen ID y nombres únicos
8. Los valores únicos se emplean directamente. En los valores que están repetidos se asigna un nombre a cada identificador en función de las veces que este aparezca
9. Se concatenean ambos dataframes (únicos y no únicos)
10. Se propone un nombre de todos los que aparecen (a revisar)
11. Expandir la información a partir de los datos:
    - Es PYME
    - Ciudad y código postal
    - Provincia y tipo de empresa
12. Búsqueda de UTEs (se puede añadir directamente como columna)
13. Cargar las empresas de Zaragoza y hacer la misma limpieza
14. Vista general:
    - Ver qué nombres e IDs están repetidos
    - Buscar en las empresas de Zaragoza en las que teníamos antes (por ID y por nombre)
    - Empresas que no tienen NIF e intentar rellenar con los NIFs que tenemos
    - Separación entre valores (ID-Name) únicos y repetidos.

[![](https://img.shields.io/badge/lang-en-red)](README.en.md)
