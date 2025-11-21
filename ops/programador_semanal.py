"""Scheduler semanal para el pipeline MCP.

Ejecuta ``etl/run_all_etl.py`` automáticamente cada lunes a la hora
configurada (por defecto 05:00). También se puede lanzar el pipeline una sola
vez con ``--run-now``.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
RUN_ALL_ETL_SCRIPT = BASE_DIR / "etl" / "run_all_etl.py"
LOG_FILE = BASE_DIR / "logs" / "programador_semanal.log"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Programa la ejecución semanal del pipeline MCP"
    )
    parser.add_argument(
        "--weekday",
        type=int,
        default=0,
        help=(
            "Día de la semana para la ejecución (0=lunes ... 6=domingo). "
            "Por defecto 0."
        ),
    )
    parser.add_argument(
        "--time",
        default="05:00",
        help="Hora HH:MM (24h) a la que debe ejecutarse el pipeline."
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Ejecuta el pipeline inmediatamente y termina.",
    )
    return parser.parse_args()


def setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def run_pipeline() -> None:
    logging.info("Iniciando pipeline semanal: %s", RUN_ALL_ETL_SCRIPT)
    result = subprocess.run(
        [sys.executable, str(RUN_ALL_ETL_SCRIPT)],
        cwd=str(BASE_DIR),
    )
    if result.returncode == 0:
        logging.info("Pipeline completado correctamente")
    else:
        logging.error("Pipeline falló con código %s", result.returncode)


def parse_time_arg(value: str) -> tuple[int, int]:
    try:
        hour_str, minute_str = value.split(":", 1)
        hour, minute = int(hour_str), int(minute_str)
    except Exception as exc:
        raise argparse.ArgumentTypeError(
            f"Hora inválida '{value}'. Usa HH:MM"
        ) from exc

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise argparse.ArgumentTypeError("Hora fuera de rango (00:00-23:59)")
    return hour, minute


def compute_next_run(
    weekday: int,
    hour: int,
    minute: int,
    now: datetime | None = None,
) -> datetime:
    now = now or datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    # ajustamos al siguiente día configurado
    days_ahead = (weekday - target.weekday()) % 7
    if days_ahead == 0 and target <= now:
        days_ahead = 7
    target += timedelta(days=days_ahead)
    return target


def run_scheduler(args: argparse.Namespace) -> None:
    weekday = args.weekday % 7
    hour, minute = parse_time_arg(args.time)

    if args.run_now:
        run_pipeline()
        return

    weekday_name = [
        "lunes",
        "martes",
        "miércoles",
        "jueves",
        "viernes",
        "sábado",
        "domingo",
    ][weekday]
    logging.info(
        "Scheduler iniciado. Corridas cada %s a las %02d:%02d",
        weekday_name,
        hour,
        minute,
    )

    while True:
        next_run = compute_next_run(weekday, hour, minute)
        wait_seconds = (next_run - datetime.now()).total_seconds()
        logging.info(
            "Próxima ejecución programada para %s (%.2f horas)",
            next_run.isoformat(timespec="seconds"),
            wait_seconds / 3600,
        )
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        run_pipeline()


def main() -> None:
    args = parse_args()
    setup_logging()
    run_scheduler(args)


if __name__ == "__main__":
    main()
