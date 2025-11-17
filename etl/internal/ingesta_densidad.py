import csv
from pathlib import Path
import time
import logging
from datetime import datetime

import requests

API_URL = "http://127.0.0.1:8000/ingesta/indicadores"

# Ruta al CSV interno de densidad
CSV_PATH = Path("data/input/densidad.csv")

MAX_RETRIES = 3

# Carpeta y archivo de logs
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"ingesta_densidad_{datetime.now():%Y%m%d_%H%M%S}.log"

def setup_logging():
    """
    Configura logging para que escriba en archivo y en consola.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Limpia handlers previos (por si se llama desde otro módulo)
    if logger.handlers:
        logger.handlers.clear()

    # Handler a archivo
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.INFO)

    # Handler a consola (opcional)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)


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
    logger = logging.getLogger(__name__)

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

                    logger.info(
                        f"[OK] Fila {idx}: fecha={payload['fecha']} "
                        f"filial={payload['filial_code']} servicio={payload['servicio_code']} "
                        f"densidad={payload['valor']}"
                    )
                    break  # éxito, dejamos de reintentar

                except Exception as e:
                    logger.warning(
                        f"[WARNING] Intento {attempt+1}/{MAX_RETRIES} falló "
                        f"para fila {idx}: {e}"
                    )

                    if attempt < MAX_RETRIES - 1:
                        # Espera incremental: 1s, 2s, 4s...
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(
                            f"[ERROR] Fila {idx} NO procesada después de "
                            f"{MAX_RETRIES} intentos."
                        )



if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info(f"Procesando archivo de densidad: {CSV_PATH}")
    process_csv(CSV_PATH)
    logger.info("Proceso densidad completado.")