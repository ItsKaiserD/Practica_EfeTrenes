import csv
import time
from pathlib import Path
from datetime import datetime
import logging
import requests

API_URL = "http://127.0.0.1:8000/ingesta/indicadores"  # tu API FastAPI interna
CSV_PATH = Path("data/input/external.csv")
MAX_RETRIES = 3

LOG_DIR = Path("data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"ingesta_externa_{datetime.now():%Y%m%d_%H%M%S}.log"


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    ch = logging.StreamHandler()

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
    Mapea una fila del CSV externo al modelo Indicador
    que recibe la API (/ingesta/indicadores).
    """
    return {
        "fecha": row["fecha"],
        "filial_code": row["filial_code"],
        "servicio_code": row["servicio_code"],
        "tipo_indicador": row["tipo_indicador"],
        "valor": float(row["valor"]),
        "fuente": "externo_csv",  # marcamos que viene de fuente externa
    }


def run():
    logger = logging.getLogger(__name__)
    logger.info("=== Iniciando HU2 - Ingesta externa desde external.csv ===")

    if not CSV_PATH.exists():
        logger.error(f"No se encontró el archivo {CSV_PATH}")
        return {"status": "error", "message": "CSV no encontrado"}

    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        filas = 0
        for idx, row in enumerate(reader, start=1):
            filas += 1
            payload = row_to_indicador(row)

            for attempt in range(MAX_RETRIES):
                try:
                    resp = requests.post(API_URL, json=payload, timeout=10)
                    resp.raise_for_status()

                    logger.info(
                        f"[OK] fila {idx}: fecha={payload['fecha']} "
                        f"filial={payload['filial_code']} "
                        f"servicio={payload['servicio_code']} "
                        f"tipo={payload['tipo_indicador']} "
                        f"valor={payload['valor']}"
                    )
                    break

                except Exception as e:
                    logger.warning(
                        f"[HU2] intento {attempt+1}/{MAX_RETRIES} fallido "
                        f"para fila {idx}: {e}"
                    )
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(
                            f"[HU2] fila {idx} descartada tras "
                            f"{MAX_RETRIES} intentos"
                        )

    logger.info(f"HU2 finalizada. Filas leídas desde external.csv: {filas}")
    return {"status": "success", "rows": filas}


if __name__ == "__main__":
    setup_logging()
    run()
