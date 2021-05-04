import csv
from datetime import datetime
from pathlib import Path

import sqlite_utils

BASE_PATH = (Path(__file__) / "..").resolve()


def transform_date(value, input_format="%Y%m%d"):
    return datetime.strptime(value, input_format).date().isoformat()


def load_registro_vacunacion_nominal():
    with (BASE_PATH / "registro_vacunacion.csv").open(
        mode="r", encoding="utf-8-sig"
    ) as csv_file:
        for row in csv.DictReader(csv_file):
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
    db = sqlite_utils.Database("registro_vacunacion.db")
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
