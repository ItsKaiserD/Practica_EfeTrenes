from fastapi import FastAPI
from datetime import datetime
import json
import os

from .models import Indicador

app = FastAPI(title="MCP API - Practica EFE Trenes")

RAW_PATH = "data/raw"


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
