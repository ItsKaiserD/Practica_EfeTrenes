"""Calculo de indicadores MCP a partir de lecturas RAW.

Lee los archivos JSON generados por la API en data/raw/<fuente>/<tipo>/YYYY=... y
agrega las métricas necesarias (densidad promedio, temperatura máxima, rango
térmico). Luego compara con los datasets de referencia ubicados en
data/reference y genera un CSV de salida en data/silver/ con el resultado y la
validación.
"""

from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


RAW_PATH = Path("data/raw")
REFERENCE_DIR = Path("data/reference")
OUTPUT_DIR = Path("data/silver")
LOG_DIR = Path("logs")
RUN_HISTORY_FILE = LOG_DIR / "calc_mcp_runs.csv"

RUN_HISTORY_FIELDS = [
    "run_id",
    "inicio",
    "fin",
    "duracion_seg",
    "lecturas_densidad",
    "lecturas_temperatura",
    "indicadores_densidad",
    "indicadores_temp_max",
    "indicadores_temp_rango",
    "total_indicadores",
    "status_ok",
    "status_desvio",
    "status_sin_referencia",
    "status_otro",
    "log_file",
    "resultado_csv",
]

REFERENCE_TOLERANCE = 1e-6


def setup_logging() -> Path:
    LOG_DIR.mkdir(exist_ok=True, parents=True)
    log_file = LOG_DIR / f"calc_mcp_{datetime.now():%Y%m%d_%H%M%S}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    return log_file


def load_raw_records(fuente: str, tipo: str) -> List[dict]:
    base_path = RAW_PATH / fuente / tipo
    if not base_path.exists():
        logging.warning("No se encontraron lecturas RAW en %s", base_path)
        return []

    records: List[dict] = []
    for json_file in base_path.rglob("*.json"):
        try:
            with json_file.open(encoding="utf-8") as fh:
                data = json.load(fh)
                data["_file"] = str(json_file)
                records.append(data)
        except Exception as exc:  # pragma: no cover (solo logs)
            logging.error("No se pudo leer %s: %s", json_file, exc)

    logging.info(
        "Lecturas RAW cargadas para %s/%s: %s archivos",
        fuente,
        tipo,
        len(records),
    )
    return records


def _group_values(records: Iterable[dict]) -> Dict[Tuple[str, str], List[float]]:
    groups: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for row in records:
        tramo = row.get("tramo_id")
        fecha = row.get("fecha")
        valor = row.get("valor")

        if not tramo or not fecha:
            logging.warning(
                "Fila sin tramo/fecha. Archivo=%s", row.get("_file", "desconocido")
            )
            continue

        try:
            valor_float = float(valor)
        except (TypeError, ValueError):
            logging.warning(
                "Valor inválido (%s) en archivo=%s", valor, row.get("_file")
            )
            continue

        groups[(tramo, fecha)].append(valor_float)

    return groups


def calc_densidad_promedio(records: List[dict]) -> List[dict]:
    indicator_id = "MCP_DENS_PROM"
    groups = _group_values(records)
    results = []
    for (tramo, fecha), values in groups.items():
        promedio = sum(values) / len(values)
        results.append(
            {
                "id_indicador": indicator_id,
                "tramo_id": tramo,
                "fecha": fecha,
                "valor_calculado": promedio,
                "muestras": len(values),
            }
        )
    logging.info(
        "Calculadas %s filas para %s", len(results), indicator_id
    )
    return results


def calc_temperatura_max(records: List[dict]) -> List[dict]:
    indicator_id = "MCP_TEMP_MAX"
    groups = _group_values(records)
    results = []
    for (tramo, fecha), values in groups.items():
        results.append(
            {
                "id_indicador": indicator_id,
                "tramo_id": tramo,
                "fecha": fecha,
                "valor_calculado": max(values),
                "muestras": len(values),
            }
        )
    logging.info(
        "Calculadas %s filas para %s", len(results), indicator_id
    )
    return results


def calc_temperatura_rango(records: List[dict]) -> List[dict]:
    indicator_id = "MCP_TEMP_RANGO"
    groups = _group_values(records)
    results = []
    for (tramo, fecha), values in groups.items():
        rango = max(values) - min(values)
        results.append(
            {
                "id_indicador": indicator_id,
                "tramo_id": tramo,
                "fecha": fecha,
                "valor_calculado": rango,
                "muestras": len(values),
            }
        )
    logging.info(
        "Calculadas %s filas para %s", len(results), indicator_id
    )
    return results


def load_reference_map(indicator_id: str) -> Dict[Tuple[str, str], float]:
    filename = REFERENCE_DIR / f"mcp_reference_{indicator_id}.csv"
    if not filename.exists():
        logging.warning("No existe dataset de referencia: %s", filename)
        return {}

    ref_map: Dict[Tuple[str, str], float] = {}
    with filename.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        value_col = reader.fieldnames[-1] if reader.fieldnames else None
        for row in reader:
            tramo = row.get("tramo_id")
            fecha = row.get("fecha")
            valor = row.get(value_col) if value_col else None
            if not tramo or not fecha or valor is None:
                continue
            try:
                ref_map[(tramo, fecha)] = float(valor)
            except ValueError:
                logging.warning(
                    "Valor de referencia inválido en %s (tramo=%s fecha=%s)",
                    filename,
                    tramo,
                    fecha,
                )
    logging.info(
        "Referencias cargadas para %s: %s filas",
        indicator_id,
        len(ref_map),
    )
    return ref_map


def attach_reference(indicator_id: str, rows: List[dict]) -> None:
    ref_map = load_reference_map(indicator_id)
    for row in rows:
        key = (row["tramo_id"], row["fecha"])
        ref_val = ref_map.get(key)
        row["valor_referencia"] = ref_val
        if ref_val is None:
            row["delta"] = None
            row["status"] = "sin_referencia"
        else:
            delta = row["valor_calculado"] - ref_val
            row["delta"] = delta
            row["status"] = (
                "ok" if abs(delta) <= REFERENCE_TOLERANCE else "desvio"
            )


def write_results(rows: List[dict]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = OUTPUT_DIR / f"mcp_indicadores_{timestamp}.csv"

    fieldnames = [
        "id_indicador",
        "tramo_id",
        "fecha",
        "valor_calculado",
        "muestras",
        "valor_referencia",
        "delta",
        "status",
    ]

    with out_file.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    logging.info("Resultados escritos en %s (%s filas)", out_file, len(rows))
    return out_file


def summarize_status(rows: List[dict]) -> Dict[str, int]:
    summary: Dict[str, int] = defaultdict(int)
    for row in rows:
        status = row.get("status") or "sin_estado"
        summary[status] += 1
    return summary


def record_run_history(
    start_time: datetime,
    end_time: datetime,
    log_file: Path,
    output_file: Path | None,
    raw_counts: Dict[str, int],
    calc_counts: Dict[str, int],
    status_summary: Dict[str, int],
) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    RUN_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)

    duration = (end_time - start_time).total_seconds()
    known_statuses = {"ok", "desvio", "sin_referencia"}
    other_status = sum(
        count
        for status, count in status_summary.items()
        if status not in known_statuses
    )

    row = {
        "run_id": start_time.strftime("%Y%m%d_%H%M%S"),
        "inicio": start_time.isoformat(timespec="seconds"),
        "fin": end_time.isoformat(timespec="seconds"),
        "duracion_seg": f"{duration:.3f}",
        "lecturas_densidad": raw_counts.get("densidad", 0),
        "lecturas_temperatura": raw_counts.get("temperatura", 0),
        "indicadores_densidad": calc_counts.get("MCP_DENS_PROM", 0),
        "indicadores_temp_max": calc_counts.get("MCP_TEMP_MAX", 0),
        "indicadores_temp_rango": calc_counts.get("MCP_TEMP_RANGO", 0),
        "total_indicadores": sum(calc_counts.values()),
        "status_ok": status_summary.get("ok", 0),
        "status_desvio": status_summary.get("desvio", 0),
        "status_sin_referencia": status_summary.get("sin_referencia", 0),
        "status_otro": other_status,
        "log_file": str(log_file),
        "resultado_csv": str(output_file) if output_file else "",
    }

    is_new = not RUN_HISTORY_FILE.exists()
    with RUN_HISTORY_FILE.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RUN_HISTORY_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    start_time = datetime.now()
    log_file = setup_logging()
    logging.info("===== Inicio cálculo MCP =====")

    results: List[dict] = []

    densidad_raw = load_raw_records("interno_densidad", "densidad")
    densidad_rows = calc_densidad_promedio(densidad_raw)
    attach_reference("MCP_DENS_PROM", densidad_rows)
    results.extend(densidad_rows)

    temperatura_raw = load_raw_records("interno_temperatura", "temperatura")
    temp_max_rows = calc_temperatura_max(temperatura_raw)
    attach_reference("MCP_TEMP_MAX", temp_max_rows)
    results.extend(temp_max_rows)

    temp_rng_rows = calc_temperatura_rango(temperatura_raw)
    attach_reference("MCP_TEMP_RANGO", temp_rng_rows)
    results.extend(temp_rng_rows)

    output_file: Path | None = None
    if results:
        output_file = write_results(results)
    else:
        logging.warning("No se generaron indicadores MCP (sin lecturas RAW)")

    end_time = datetime.now()
    calc_counts = {
        "MCP_DENS_PROM": len(densidad_rows),
        "MCP_TEMP_MAX": len(temp_max_rows),
        "MCP_TEMP_RANGO": len(temp_rng_rows),
    }
    raw_counts = {
        "densidad": len(densidad_raw),
        "temperatura": len(temperatura_raw),
    }
    status_summary = summarize_status(results)
    record_run_history(
        start_time,
        end_time,
        log_file,
        output_file,
        raw_counts,
        calc_counts,
        status_summary,
    )

    logging.info("===== Fin cálculo MCP =====")


if __name__ == "__main__":
    main()
