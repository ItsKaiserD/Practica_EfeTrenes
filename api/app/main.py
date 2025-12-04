from datetime import datetime
import json
import logging
import subprocess
import sys
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.background import BackgroundTasks

from .models import Indicador

app = FastAPI(title="MCP API - Practica EFE Trenes")

RAW_PATH = Path("data/raw")

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

    subprocess.Popen(
        [sys.executable, str(RUN_ALL_ETL_SCRIPT)],
        cwd=str(BASE_DIR),
    )

def save_raw_file(indicador: Indicador):
    """Persistir cada payload en rutas particionadas por fuente/tipo/fecha."""

    fecha = indicador.fecha
    fuente = indicador.fuente or "desconocido"
    tipo = indicador.tipo_indicador or "sin_tipo"

    folder = (
        RAW_PATH
        / fuente
        / tipo
        / f"YYYY={fecha.year}"
        / f"MM={fecha.month:02d}"
        / f"DD={fecha.day:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = folder / f"indicadores_{timestamp}.json"

    payload = indicador.model_dump()
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
    """
    background_tasks.add_task(_run_all_etl_job)

    return {
        "status": "accepted",
        "job": "run_all_etl",
        "detail": "El ETL HU1 + HU2 se está ejecutando en segundo plano.",
        "script": str(RUN_ALL_ETL_SCRIPT),
    }



# ================================================================
# !!!!!!!!RELACIONADO A LA BASE DE DATOS
# ================================================================

from .models import indicadorSchema   
from .database import engine, Base, SessionLocal
from sqlalchemy.orm import Session

from internal.ingesta_densidad import procesar_densidad
from internal.ingesta_temperatura import procesar_temperatura
from internal.ingesta_viajes_validados import procesar_viajes_validados


# Crear tablas si no existen
Base.metadata.create_all(bind=engine)


# Sesión DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# GUARDAR DENSIDAD
@app.post("/guardar/densidad")
def guardar_densidad(db: Session = Depends(get_db)):
    data = procesar_densidad()

    for fila in data:
        registro = indicadorSchema(
            tipo="densidad",
            fecha=fila["fecha"],
            valor=fila.get("valor") or fila.get("densidad") or None,
            metadata=fila,
        )
        db.add(registro)

    db.commit()
    return {"status": "ok", "registros_insertados": len(data)}


# GUARDAR TEMPERATURA
@app.post("/guardar/temperatura")
def guardar_temperatura(db: Session = Depends(get_db)):
    data = procesar_temperatura()

    for fila in data:
        registro = indicadorSchema(
            tipo="temperatura",
            fecha=fila["fecha"],
            valor=fila.get("valor") or fila.get("temperatura") or None,
            metadata=fila,
        )
        db.add(registro)

    db.commit()
    return {"status": "ok", "registros_insertados": len(data)}

# GUARDAR VIAJES VALIDADOS
@app.post("/guardar/viajes_validados")
def guardar_viajes_validados(db: Session = Depends(get_db)):
    data = procesar_viajes_validados()

    for fila in data:
        registro = indicadorSchema(
            tipo="viajes_validados",
            fecha=fila["fecha"],
            valor=fila.get("valor") or fila.get("viajes_validados") or None,
            metadata=fila,
        )
        db.add(registro)

    db.commit()
    return {"status": "ok", "registros_insertados": len(data)}
