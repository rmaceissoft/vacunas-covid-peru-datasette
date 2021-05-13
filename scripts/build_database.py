import csv
from datetime import datetime
from pathlib import Path

import pandas as pd
import sqlite_utils

from utils import strip_accents_spain


BASE_PATH = (Path(__file__) / "../..").resolve()


DISTRICTS_MAPPING = {
    "AMAZONAS,LUYA,SAN FRANCISCO DEL YESO": (
        "AMAZONAS",
        "LUYA",
        "SAN FRANCISCO DE YESO",
    ),
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
    "LIMA,LIMA,LURIGANCHO (CHOSICA)": ("LIMA", "LIMA", "LURIGANCHO"),
    "LIMA,LIMA,MAGDALENA VIEJA (PUEBLO LIBRE)": ("LIMA", "LIMA", "PUEBLO LIBRE"),
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


def load_registro_vacunacion_nominal():
    with (BASE_PATH / "data/registro_vacunacion.csv").open(
        mode="r", encoding="utf-8-sig"
    ) as csv_file:
        data_frame = pd.read_csv(
            BASE_PATH / "data/distritos_peru.csv", dtype={"ubigeo": str}
        )
        index = 0
        for row in csv.DictReader(csv_file):
            index += 1
            print(f"INFO: processing record {index}")
            _key = ",".join([row["DEPARTAMENTO"], row["PROVINCIA"], row["DISTRITO"]])
            q_department, q_province, q_district = DISTRICTS_MAPPING.get(
                _key,
                (
                    strip_accents_spain(row["DEPARTAMENTO"]),
                    strip_accents_spain(row["PROVINCIA"]),
                    strip_accents_spain(row["DISTRITO"]),
                ),
            )
            results = data_frame.query(
                f'departamento == "{q_department}" & provincia == "{q_province}" & distrito == "{q_district}"'
            )
            if results.empty:
                latitude, longitude, ubigeo = None, None, None
                print(
                    f"WARNING: Distrito no encontrado: {q_department} -> {q_province} -> {q_district}"
                )
            else:
                latitude = results.values[0][3]
                longitude = results.values[0][4]
                ubigeo = results.values[0][5]
            yield {
                "fecha_corte": transform_date(row["FECHA_CORTE"]),
                "uuid": row["UUID"],
                "grupo_riesgo": row["GRUPO_RIESGO"],
                "edad": row["EDAD"],
                "sexo": row["SEXO"],
                "fecha_vacunacion": transform_date(row["FECHA_VACUNACION"]),
                "dosis": row["DOSIS"],
                "fabricante": row["FABRICANTE"],
                "diresa": row["DIRESA"],
                "departamento": row["DEPARTAMENTO"],
                "provincia": row["PROVINCIA"],
                "distrito": row["DISTRITO"],
                "latitude": latitude,
                "longitude": longitude,
                "distrito_ubigeo": ubigeo,
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
            "total_vacunados_nuevos_segunda_dosis": nuevos_segunda_dosis,
            "total_vacunados_acumulados_segunda_dosis": acumulados_segunda_dosis,
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
