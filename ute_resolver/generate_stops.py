import pathlib
from collections import Counter
import numpy as np
import pandas as pd


def flatten(xss):
    return [x for xs in xss for x in xs]


path_data = pathlib.Path(
    "/export/usuarios_ml4ds/lbartolome/NextProcurement/NP-Company-Process/data")
path_resolved = path_data.joinpath("utes_spark4.parquet")
df = pd.read_parquet(path_resolved)


df["len"] = df.utes.apply(len)
splits_list = df[df.len > 0]['splits'].values.tolist()
splits_list = flatten(splits_list)

# Si aparecen en muchas utes, no definen la empresa
frequency_counter = Counter(splits_list)
upper_limit = 95
per_upper = np.percentile(list(frequency_counter.values()), upper_limit)

print(f"Upper limit: {per_upper}")

filtered_elements = [
    item for item, count in frequency_counter.items() if
    count > per_upper
]

print(f"Keeping {len(filtered_elements)} elements out of {len(splits_list)}.")

additional_elements = [
    "UTE", "ute", "u.t.e.", "servicio", "servicios", "obras",
    "fundación", "información", "técnica", "proyectos", "y",
    "engineering", "architecture", "technology", "solutions", "infraestructuras", "inst", "contrucciones", "construccions", "office", "spain", "associacio", "formacio", "arquitectes", "gestio", "information", "international", "systems", "system", "excavacions", "facility", "partners", "consulting", "catalunya", "constr", "projects", "intelligence", "educatio", "systems", "electrodomesticos", "marketing", "extremadura", "networks", "estacionamientos", "management", "gipuzkoa", "coslada", "security", "project", "design", "studio", "security", "service", "avda", "serv", "trans", "quality", "group", "services", "asturias", "manteniments", "investment", "quality", "cantabria", "medioambientales", "sist", "energy", "rehabilitaciones", "bizkaia", "research", "enginyeria", "electronics", "solucions", "facilities", "music", "technologies"]

final_list = list(set(filtered_elements + additional_elements))
final_list.sort()
print(f"Final list has {len(final_list)} elements.")

path_save = path_data.joinpath("filtered_elements.txt")
with open(path_save, 'w') as file:
    for item in final_list:
        file.write("%s\n" % item)

