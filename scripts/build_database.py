import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd
import sqlite_utils

from utils import read_csv_from_url, strip_accents_spain


BASE_PATH = (Path(__file__) / "../..").resolve()
# Electoral population (18 years or older) of the 2021 general election process
# published by RENIEC (excluding residents abroad)
TOTAL_POPULATION = 24290921


DISTRICTS_MAPPING = {
    "AMAZONAS,LUYA,SAN FRANCISCO DEL YESO": (
        "AMAZONAS",
        "LUYA",
        "SAN FRANCISCO DE YESO",
    ),
    "APURIMAC,AYMARAES,HUAYLLO": ("APURIMAC", "AYMARAES", "IHUAYLLO"),
    "APURIMAC,CHINCHEROS,ANCO-HUALLO": ("APURIMAC", "CHINCHEROS", "ANCO_HUALLO"),
    "CALLAO,CALLAO,CALLAO": ("CALLAO", "PROV. CONST. DEL CALLAO", "CALLAO"),
    "CALLAO,CALLAO,BELLAVISTA": ("CALLAO", "PROV. CONST. DEL CALLAO", "BELLAVISTA"),
    "CALLAO,CALLAO,CARMEN DE LA LEGUA REYNOSO": (
        "CALLAO",
        "PROV. CONST. DEL CALLAO",
        "CARMEN DE LA LEGUA REYNOSO",
    ),
    "CALLAO,CALLAO,LA PERLA": ("CALLAO", "PROV. CONST. DEL CALLAO", "LA PERLA"),
    "CALLAO,CALLAO,LA PUNTA": ("CALLAO", "PROV. CONST. DEL CALLAO", "LA PUNTA"),
    "CALLAO,CALLAO,VENTANILLA": ("CALLAO", "PROV. CONST. DEL CALLAO", "VENTANILLA"),
    "CALLAO,CALLAO,MI PERU": ("CALLAO", "PROV. CONST. DEL CALLAO", "MI PERU"),
    "HUANUCO,HUANUCO,QUISQUI": ("HUANUCO", "HUANUCO", "QUISQUI (KICHKI)"),
    "ICA,NAZCA,CHANGUILLO": ("ICA", "NASCA", "CHANGUILLO"),
    "ICA,NAZCA,EL INGENIO": ("ICA", "NASCA", "EL INGENIO"),
    "ICA,NAZCA,MARCONA": ("ICA", "NASCA", "MARCONA"),
    "ICA,NAZCA,NAZCA": ("ICA", "NASCA", "NASCA"),
    "ICA,NAZCA,VISTA ALEGRE": ("ICA", "NASCA", "VISTA ALEGRE"),
    "JUNIN,CHANCHAMAYO,PICHANAKI": ("JUNIN", "CHANCHAMAYO", "PICHANAQUI"),
    "JUNIN,CONCEPCION,SANTO DOMINGO DE ACOBAMBA": (
        "JUNIN",
        "HUANCAYO",
        "SANTO DOMINGO DE ACOBAMBA",
    ),
    "LIMA,LIMA,LURIGANCHO (CHOSICA)": ("LIMA", "LIMA", "LURIGANCHO"),
    "LIMA,LIMA,MAGDALENA VIEJA (PUEBLO LIBRE)": ("LIMA", "LIMA", "PUEBLO LIBRE"),
    "LIMA,YAUYOS,AYAUCA": ("LIMA", "YAUYOS", "ALLAUCA"),
    "PIURA,PIURA,VEINTISEIS DE OCTUB": ("PIURA", "PIURA", "VEINTISEIS DE OCTUBRE"),
    "PUNO,SAN ROMAS,SAN MIGUEL": ("PUNO", "SAN ROMAN", "SAN MIGUEL"),
    "UCAYALI,PADRE ABAD,ALEXANDER VON HUMBO": (
        "UCAYALI",
        "PADRE ABAD",
        "ALEXANDER VON HUMBOLDT",
    ),
    "PASCO,OXAPAMPA,CONSTITUCIÓN": ("PASCO", "OXAPAMPA", "CONSTITUCION"),
}


def transform_date(value, input_format="%Y%m%d"):
    return datetime.strptime(value, input_format).date().isoformat()


def get_hash_value(value):
    hash_object = hashlib.sha256(value.encode())
    return hash_object.hexdigest()


def normalize_and_hash(row):
    value = ",".join([row["DEPARTAMENTO"], row["PROVINCIA"], row["DISTRITO"]])
    department, province, district = DISTRICTS_MAPPING.get(
        value,
        (
            strip_accents_spain(row["DEPARTAMENTO"]),
            strip_accents_spain(row["PROVINCIA"]),
            strip_accents_spain(row["DISTRITO"]),
        ),
    )
    return get_hash_value(",".join([department, province, district]))


def load_registro_vacunacion_nominal():
    df_vaccination = read_csv_from_url(
        "https://cloud.minsa.gob.pe/s/ZgXoXqK2KLjRLxD/download",
        dtype={"FECHA_CORTE": str, "FECHA_VACUNACION": str, "EDAD": "Int64"},
    )
    df_districts = pd.read_csv(
        BASE_PATH / "data/distritos_peru.csv", dtype={"ubigeo": str}
    )
    # add a new column with a hash of Department,Province,District, which will be used
    # to merge the district lat, long and ubigeo with the nominal vaccination records
    df_vaccination["hash_value"] = df_vaccination.apply(normalize_and_hash, axis=1)
    df_districts["hash_value"] = df_districts.apply(
        lambda row: get_hash_value(
            ",".join([row["departamento"], row["provincia"], row["distrito"]])
        ),
        axis=1,
    )
    df_vaccination = df_vaccination.merge(
        df_districts[["latitud", "longitud", "ubigeo", "hash_value"]],
        how="left",
        left_on="hash_value",
        right_on="hash_value",
    ).drop(columns=["hash_value"])
    for index, row in df_vaccination.iterrows():
        yield {
            "fecha_corte": transform_date(row["FECHA_CORTE"]),
            "uuid": row["UUID"],
            "grupo_riesgo": row["GRUPO_RIESGO"],
            "edad": row["EDAD"] if not pd.isnull(row["EDAD"]) else None,
            "sexo": row["SEXO"],
            "fecha_vacunacion": transform_date(row["FECHA_VACUNACION"]),
            "dosis": row["DOSIS"],
            "fabricante": row["FABRICANTE"],
            "diresa": row["DIRESA"],
            "departamento": row["DEPARTAMENTO"],
            "provincia": row["PROVINCIA"],
            "distrito": row["DISTRITO"],
            "latitud": row["latitud"],
            "longitud": row["longitud"],
            "distrito_ubigeo": row["ubigeo"],
        }


def calculate_totales_por_fecha_y_dosis(db, fecha_vacunacion, dosis):
    nuevos = db.execute(
        """
        SELECT count(*) 
        FROM registro_vacunacion_nominal 
        WHERE fecha_vacunacion = :fecha_vacunacion AND dosis = :dosis 
        """,
        {"fecha_vacunacion": fecha_vacunacion, "dosis": dosis},
    ).fetchone()[0]
    acumulados = db.execute(
        """
        SELECT count(*) 
        FROM registro_vacunacion_nominal 
        WHERE fecha_vacunacion <= :fecha_vacunacion AND dosis = :dosis 
        """,
        {"fecha_vacunacion": fecha_vacunacion, "dosis": dosis},
    ).fetchone()[0]
    return nuevos, acumulados


def load_registro_vacunacion_diaria(db):
    # generate daily vaccination records
    for row in db.execute(
        """
        SELECT DISTINCT fecha_vacunacion
        FROM registro_vacunacion_nominal
        ORDER BY fecha_vacunacion DESC
    """
    ).fetchall():
        fecha_vacunacion = row[0]
        (
            nuevos_primera_dosis,
            acumulados_primera_dosis,
        ) = calculate_totales_por_fecha_y_dosis(db, fecha_vacunacion, 1)
        (
            nuevos_segunda_dosis,
            acumulados_segunda_dosis,
        ) = calculate_totales_por_fecha_y_dosis(db, fecha_vacunacion, 2)
        yield {
            "fecha_vacunacion": fecha_vacunacion,
            "total_vacunados_nuevos_primera_dosis": nuevos_primera_dosis,
            "total_vacunados_acumulados_primera_dosis": acumulados_primera_dosis,
            "porciento_vacunados_primera_dosis": round(
                (acumulados_primera_dosis / TOTAL_POPULATION) * 100, 2
            ),
            "total_vacunados_nuevos_segunda_dosis": nuevos_segunda_dosis,
            "total_vacunados_acumulados_segunda_dosis": acumulados_segunda_dosis,
            "porciento_vacunados_segunda_dosis": round(
                (acumulados_segunda_dosis / TOTAL_POPULATION) * 100, 2
            ),
        }


def main():
    db = sqlite_utils.Database(BASE_PATH / "data/registro_vacunacion.db")
    table = db["registro_vacunacion_nominal"]
    if table.exists():
        table.drop()
    table.insert_all(load_registro_vacunacion_nominal(), pk=("uuid", "dosis"))
    table.create_index(["grupo_riesgo"], if_not_exists=True)
    table.create_index(["sexo"], if_not_exists=True)
    table.create_index(["fabricante"], if_not_exists=True)
    table.create_index(["diresa"], if_not_exists=True)
    table.create_index(["departamento"], if_not_exists=True)
    table.create_index(["provincia"], if_not_exists=True)
    table.create_index(["distrito"], if_not_exists=True)

    table = db["registro_vacunacion_diario"]
    if table.exists():
        table.drop()
    table.insert_all(load_registro_vacunacion_diaria(db), pk="fecha_vacunacion")


if __name__ == "__main__":
    main()
