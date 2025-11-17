from fastapi import FastAPI
from datetime import datetime
import json
import os
import subprocess
import sys
from pathlib import Path
import logging
from fastapi.background import BackgroundTasks

from .models import Indicador

app = FastAPI(title="MCP API - Practica EFE Trenes")

RAW_PATH = "data/raw"

# Ruta base del proyecto (carpeta raíz, por encima de api/)
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Script ETL que orquesta HU1 + HU2
RUN_ALL_ETL_SCRIPT = BASE_DIR / "etl" / "run_all_etl.py"

def _run_all_etl_job():
    """
    Ejecuta el ETL completo (HU1 + HU2) como un proceso separado.
    Se usa desde el endpoint /jobs/etl/run-all en background.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    logging.info(f"Lanzando run_all_etl.py: {RUN_ALL_ETL_SCRIPT}")

    # Lanza el script ETL en un proceso aparte sin bloquear la API
    subprocess.Popen(
        [sys.executable, str(RUN_ALL_ETL_SCRIPT)],
        cwd=str(BASE_DIR),
    )

def save_raw_file(indicador: Indicador):
    # indicador.fecha ya es un `date`
    fecha = indicador.fecha

    # Carpeta por fecha
    folder = os.path.join(
        RAW_PATH,
        str(fecha.year),
        str(fecha.month),
        str(fecha.day),
    )
    os.makedirs(folder, exist_ok=True)

    # Nombre del archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = os.path.join(folder, f"indicadores_{timestamp}.json")

    # Convertimos el modelo a dict y pasamos fecha a string
    payload = indicador.model_dump()  # en Pydantic v2 (también sirve .dict())
    payload["fecha"] = fecha.isoformat()

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, ensure_ascii=False)

    return filename, payload


@app.get("/health")
def health():
    return {"status": "ok", "message": "API MCP funcionando"}


@app.post("/ingesta/indicadores")
def ingesta_indicadores(payload: Indicador):
    """HU1 / HU2: recibir indicadores y guardarlos en RAW local."""
    file_path, payload_dict = save_raw_file(payload)

    return {
        "status": "received",
        "raw_file": file_path,
        "payload": payload_dict,
    }

@app.post("/jobs/etl/run-all")
def trigger_run_all_etl(background_tasks: BackgroundTasks):
    """
    Endpoint para disparar el ETL completo (HU1 + HU2) desde la API.

    - Lanza etl/run_all_etl.py en background.
    - Devuelve inmediatamente una respuesta de "accepted".
    """
    background_tasks.add_task(_run_all_etl_job)

    return {
        "status": "accepted",
        "job": "run_all_etl",
        "detail": "El ETL HU1 + HU2 se está ejecutando en segundo plano.",
        "script": str(RUN_ALL_ETL_SCRIPT),
    }

