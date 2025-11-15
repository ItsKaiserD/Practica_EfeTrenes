import csv
from pathlib import Path

import requests

API_URL = "http://127.0.0.1:8000/ingesta/indicadores"

# Ruta al CSV interno de densidad
CSV_PATH = Path("data/input/densidad.csv")


def row_to_indicador(row: dict) -> dict:
    """
    HU1: transformar una fila del CSV de densidad al formato estándar MCP.
    """
    return {
        "fecha": row["fecha"],                  # "2025-11-01"
        "filial_code": row["filial_code"],      # "VA"
        "servicio_code": row["servicio_code"],  # "01"
        "tipo_indicador": "densidad",
        "valor": float(row["densidad"]),
        "fuente": "interno_densidad",
    }


def process_csv(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo CSV: {path}")

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            payload = row_to_indicador(row)

            response = requests.post(API_URL, json=payload)
            if response.status_code != 200:
                print(f"[ERROR] Fila {idx}: {response.status_code} -> {response.text}")
            else:
                print(
                    f"[OK] Fila {idx}: fecha={payload['fecha']} "
                    f"filial={payload['filial_code']} servicio={payload['servicio_code']} "
                    f"densidad={payload['valor']}"
                )


if __name__ == "__main__":
    print(f"Procesando archivo de densidad: {CSV_PATH}")
    process_csv(CSV_PATH)
    print("Proceso densidad completado.")
