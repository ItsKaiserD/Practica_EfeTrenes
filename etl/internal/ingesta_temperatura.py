import csv
from pathlib import Path
import time

import requests

API_URL = "http://127.0.0.1:8000/ingesta/indicadores"

# Ruta al CSV interno de temperatura
CSV_PATH = Path("data/input/temperatura.csv")

MAX_RETRIES = 3

def row_to_indicador(row: dict) -> dict:
    """
    HU1: transformar una fila del CSV de temperatura al formato estándar MCP.
    """
    return {
        "fecha": row["fecha"],                  # "2025-11-01"
        "filial_code": row["filial_code"],      # "VA"
        "servicio_code": row["servicio_code"],  # "01"
        "tipo_indicador": "temperatura",
        "valor": float(row["temperatura"]),
        "fuente": "interno_temperatura",
    }


def process_csv(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo CSV: {path}")

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            payload = row_to_indicador(row)

            for attempt in range(MAX_RETRIES):
                try:
                    response = requests.post(API_URL, json=payload, timeout=10)
                    response.raise_for_status()

                    print(
                        f"[OK] Fila {idx}: fecha={payload['fecha']} "
                        f"filial={payload['filial_code']} servicio={payload['servicio_code']} "
                        f"temperatura={payload['valor']}"
                    )
                    break  # éxito, dejamos de reintentar

                except Exception as e:
                    print(
                        f"[WARNING] Intento {attempt+1}/{MAX_RETRIES} falló para fila {idx}: {e}"
                    )

                    if attempt < MAX_RETRIES - 1:
                        # Espera incremental: 1s, 2s, 4s...
                        time.sleep(2 ** attempt)
                    else:
                        print(
                            f"[ERROR] Fila {idx} NO procesada después de {MAX_RETRIES} intentos."
                )


if __name__ == "__main__":
    print(f"Procesando archivo de temperatura: {CSV_PATH}")
    process_csv(CSV_PATH)
    print("Proceso temperatura completado.")
