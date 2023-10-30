# NextProcurement

# Funcionalidad
Este proyecto proporciona notebooks para procesar e identificar licitaciones y empresas.

# Estructura de ficheros
## Datos:
El directorio definido con los datos utilizados por la aplicación es por defecto: **_data_**. Dentro de ese directorio se encontrarán otros subdirectorios, principalmente metadata. Por ejemplo:
```
data
└───metadata
    ├───insiders.parquet
    ├───outsiders.parquet
    └───minors.parquet
```
## app/src
Contiene las diversas funcionalidades de la aplicación, divididas en companies, nif_validation y utils.

### companies:
Contiene funciones para identificar el tipo de empresa y unificar su escritura: se establece todo con sus siglas; por ejemplo: *sociedad limitada*, *soc. limitada*, *s. l.* se convierten en *s.l.*. Permite también eliminarlo del texto si se desea.

### nif_validation
Permite comprobar si los distintos identificadores (DNI, NIE y CIF) son correctos, así como identificar cuál se usa y obtener los datos en base a ello.\
Se ha usado para crear estos métodos la información que aparecen en el BOE, principalmente a través de las referencias de https://es.wikipedia.org/wiki/N%C3%BAmero_de_identificaci%C3%B3n_fiscal

### utils
Aquí se encuentran funciones con diversas finalidades, como paralelizar o cargar datos.

## Notebooks
### match_tender.ipynb
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

### match_companies.ipynb
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
